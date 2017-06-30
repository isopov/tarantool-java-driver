package com.sopovs.moradanen.tarantool;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;

//TODO finalize equivalent via PhantomReference
public class TarantoolClientImpl implements TarantoolClient {
	private final Socket socket;
	private final MessageUnpacker unpacker;
	private final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

	private final MessageBufferPacker queryPacker = MessagePack.newDefaultBufferPacker();
	private int querySize = 0;

	private final DataOutputStream out;
	private int counter;
	private Result last;
	private int batchSize = 0;

	private byte currentQuery = 0;

	private static final byte INSERT = 1;
	private static final byte UPSERT_TUPLE = 2;
	private static final byte UPSERT_OPS = 3;
	private static final byte EVAL = 4;
	private static final byte SELECT = 5;
	private static final byte DELETE = 6;
	private static final byte UPDATE_KEY = 7;
	private static final byte UPDATE_TUPLE = 8;

	private static int currentQueryToQueryCode(byte currentQuery) {
		switch (currentQuery) {
		case EVAL:
		case INSERT:
		case UPDATE_TUPLE:
		case UPSERT_TUPLE:
			return Util.KEY_TUPLE;
		case SELECT:
		case DELETE:
		case UPDATE_KEY:
			return Util.KEY_KEY;
		case UPSERT_OPS:
			return Util.KEY_UPSERT_OPS;
		case 0:
		default:
			throw new IllegalArgumentException();
		}
	}

	public TarantoolClientImpl(TarantoolConfig config) {
		this(config.getHost(), config.getPort());
	}

	public TarantoolClientImpl(String host) {
		this(host, 3301);
	}

	public TarantoolClientImpl(String host, int port) {
		this(createSocket(host, port));

	}

	private static Socket createSocket(String host, int port) {
		try {
			return new Socket(host, port);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	public TarantoolClientImpl(Socket socket) {
		this.socket = socket;
		try {
			unpacker = MessagePack.newDefaultUnpacker(socket.getInputStream());
			out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			connect();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private Result getSingleResult() {
		try {
			int bodySize = flushAndGetResultSize(true);
			if (1 != bodySize) {
				throw new TarantoolException("Body size is " + bodySize);
			}

			byte bodyKey = unpacker.unpackByte();
			if (bodyKey == Util.KEY_DATA) {
				return last = new Result(unpacker);
			} else if (bodyKey == Util.KEY_ERROR) {
				throw new TarantoolException(unpacker.unpackString());
			} else {
				throw new TarantoolException("Unknown body Key " + bodyKey);
			}
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public Result execute() {
		if (batchSize > 0) {
			executeBatch();
		}
		finishQueryWithArguments();
		return getSingleResult();
	}

	private int flushAndGetResultSize(boolean batch) throws IOException {
		out.flush();

		// TODO expose byte size to Result?
		unpacker.unpackInt();
		unpackHeader(batch);
		int bodySize = unpacker.unpackMapHeader();
		return bodySize;
	}

	@Override
	public void addBatch() {
		finishQueryWithArguments();
		batchSize++;
	}

	private void finishQueryWithArguments() {
		try {
			writeQuery();
			finishQuery();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private void writeQuery() throws IOException {
		packer.packInt(currentQueryToQueryCode(currentQuery));
		packer.packArrayHeader(querySize);
		if (querySize > 0) {
			byte[] query = queryPacker.toByteArray();
			packer.addPayload(query);
		}
		currentQuery = 0;
		querySize = 0;
		queryPacker.clear();
	}

	@Override
	public void executeBatch() {
		for (int i = 0; i < batchSize; i++) {
			Result result = getSingleResult();
			result.consume();
		}
		batchSize = 0;
	}

	@Override
	public void eval(String expression) {
		try {
			writeCode(Util.CODE_EVAL);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_EXPRESSION);
			packer.packString(expression);
			currentQuery = EVAL;
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void select(int space, int index, int limit, int offset, Iter iterator) {
		selectInternal(6, space, limit, offset, iterator);

		try {
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void selectAll(int space, int limit, int offset) {
		selectInternal(5, space, limit, offset, Iter.ALL);
	}

	private void selectInternal(int headSize, int space, int limit, int offset, Iter iterator) {
		try {
			writeCode(Util.CODE_SELECT);
			if (offset == 0) {
				headSize--;
			}
			if (iterator == Iter.EQ) {
				headSize--;
			}

			packer.packMapHeader(headSize);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);

			if (iterator != Iter.EQ) {
				packer.packInt(Util.KEY_ITERATOR);
				packer.packInt(iterator.getValue());
			}
			assert currentQuery == 0;
			currentQuery = SELECT;
			packer.packInt(Util.KEY_LIMIT);
			packer.packInt(limit);
			if (offset != 0) {
				packer.packInt(Util.KEY_OFFSET);
				packer.packInt(offset);
			}
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private void finishQuery() throws IOException {
		List<MessageBuffer> bufferList = packer.toBufferList();
		writeSize(bufferList);
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			out.write(messageBuffer.toByteArray());
		}
		packer.clear();
	}

	private void writeCode(int code) throws IOException {
		if (last != null && last.hasNext()) {
			throw new TarantoolException("Sending next without reading previous");
		}
		packer.packMapHeader(2);
		packer.packInt(Util.KEY_CODE);
		packer.packInt(code);
		packer.packInt(Util.KEY_SYNC);
		packer.packInt(++counter);
	}

	private void writeSize(List<MessageBuffer> bufferList) throws IOException {
		out.writeByte(MessagePack.Code.UINT32);
		int size = 0;
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			size += messageBuffer.size();
		}
		out.writeInt(size);
	}

	private void unpackHeader(boolean batch) throws IOException {
		int headerSize = unpacker.unpackMapHeader();
		for (int i = 0; i < headerSize; i++) {
			byte key = unpacker.unpackByte();
			if (key == Util.KEY_SYNC) {
				int sync = unpacker.unpackInt();
				if (batch) {
					if (sync > counter) {
						throw new TarantoolException("Expected sync <= " + counter + " and came " + sync);
					}
				} else if (sync != counter) {
					throw new TarantoolException("Expected sync = " + counter + " and came " + sync);
				}
			} else {
				unpacker.unpackInt();
			}
		}
	}

	private void connect() throws IOException {
		unpacker.readPayload(ByteBuffer.allocate(128));
	}

	@Override
	public void close() {
		try {
			socket.close();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void insert(int space) {
		insertOrReplace(Util.CODE_INSERT, space);
	}

	@Override
	public void replace(int space) {
		insertOrReplace(Util.CODE_REPLACE, space);
	}

	private void insertOrReplace(int code, int space) {
		try {
			writeCode(code);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			assert currentQuery == 0;
			currentQuery = INSERT;
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void delete(int space, int index) {
		try {
			writeCode(Util.CODE_DELETE);
			packer.packMapHeader(3);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			assert currentQuery == 0;
			currentQuery = DELETE;
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void ping() {
		try {
			writeCode(Util.CODE_PING);
			finishQuery();

			int bodySize = flushAndGetResultSize(false);
			if (bodySize != 0) {
				throw new TarantoolException(bodySize + " body size came from ping");
			}
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setNull() {
		try {
			querySize++;
			queryPacker.packNil();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}

	}

	@Override
	public void setLong(long val) {
		try {
			querySize++;
			queryPacker.packLong(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setBoolean(boolean val) {
		try {
			querySize++;
			queryPacker.packBoolean(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setDouble(double val) {
		try {
			querySize++;
			queryPacker.packDouble(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}

	}

	@Override
	public void setFloat(float val) {
		try {
			querySize++;
			queryPacker.packFloat(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setInt(int val) {
		try {
			querySize++;
			queryPacker.packInt(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}

	}

	@Override
	public void setString(String val) {
		try {
			querySize++;
			queryPacker.packString(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void update(int space, int index) {
		try {
			writeCode(Util.CODE_UPDATE);
			packer.packMapHeader(4);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			assert currentQuery == 0;
			currentQuery = UPDATE_KEY;
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void upsert(int space) {
		try {
			writeCode(Util.CODE_UPSERT);
			packer.packMapHeader(3);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			assert currentQuery == 0;
			currentQuery = UPSERT_TUPLE;
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void change(Op op, int field, int arg) {
		try {
			if (currentQuery == UPDATE_KEY) {
				writeQuery();
				currentQuery = UPDATE_TUPLE;
			} else if (currentQuery == UPSERT_TUPLE) {
				writeQuery();
				currentQuery = UPSERT_OPS;
			}
			querySize++;
			queryPacker.packArrayHeader(3);
			queryPacker.packString(op.getVal());
			queryPacker.packInt(field);
			queryPacker.packInt(arg);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

}
