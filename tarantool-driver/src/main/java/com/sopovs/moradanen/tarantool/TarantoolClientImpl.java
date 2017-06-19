package com.sopovs.moradanen.tarantool;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;

public class TarantoolClientImpl implements TarantoolClient, Closeable {
	private final Socket socket;
	private final MessageUnpacker unpacker;
	private final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

	private final MessageBufferPacker queryPacker = MessagePack.newDefaultBufferPacker();
	private int querySize = 0;
	private int queryCode = 0;

	private final DataOutputStream out;
	private int counter;
	private Result last;
	private int batchSize = 0;

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

	private Result execute(boolean batch) {
		try {
			packer.packInt(queryCode);
			packer.packArrayHeader(querySize);
			packer.addPayload(queryPacker.toByteArray());
			finishQuery();

			int bodySize = flushAndGetResultSize(batch);
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
	
	private Result getSingleBatchResult(){
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
		return execute(false);
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
		try {
			packer.packInt(queryCode);
			packer.packArrayHeader(querySize);
			packer.addPayload(queryPacker.toByteArray());
			finishQuery();
			batchSize++;
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void executeBatch() {
		for (int i = 0; i < batchSize; i++) {
			Result result = getSingleBatchResult();
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
			queryCode = Util.KEY_TUPLE;
			// packer.packInt(Util.KEY_TUPLE);
			// tupleWriter.writeTuple(tuple);
			// finishQuery();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void select(int space, int index, int limit, int offset) {
		try {
			writeCode(Util.CODE_SELECT);

			packer.packMapHeader(6);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);

			// TODO
			packer.packInt(Util.KEY_ITERATOR);
			packer.packInt(0);
			queryCode = Util.KEY_KEY;
			// packer.packInt(Util.KEY_KEY);
			// keyWriter.writeTuple(tuple);
			packer.packInt(Util.KEY_LIMIT);
			packer.packInt(limit);

			packer.packInt(Util.KEY_OFFSET);
			packer.packInt(offset);

			// finishQuery();
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
		queryCode = 0;
		querySize = 0;
		queryPacker.clear();

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
			queryCode = Util.KEY_TUPLE;
			// packer.packInt(Util.KEY_TUPLE);
			// tupleWriter.writeTuple(tuple);
			// finishQuery();
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
			queryCode = Util.KEY_KEY;
			// packer.packInt(Util.KEY_KEY);
			// keyWriter.writeTuple(tuple);
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);
			// finishQuery();
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

}
