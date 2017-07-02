package com.sopovs.moradanen.tarantool;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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
		this(config.getHost(), config.getPort(), config.getUsername(), config.getPassword());
	}

	public TarantoolClientImpl(String host) {
		this(host, 3301);
	}

	public TarantoolClientImpl(String host, int port) {
		this(createSocket(host, port));
	}

	public TarantoolClientImpl(String host, int port, String login, String password) {
		this(createSocket(host, port), login, password);
	}

	private static Socket createSocket(String host, int port) {
		try {
			return new Socket(host, port);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	public TarantoolClientImpl(Socket socket) {
		this(socket, null, null);
	}

	public TarantoolClientImpl(Socket socket, String login, String password) {
		this.socket = socket;
		try {
			unpacker = MessagePack.newDefaultUnpacker(socket.getInputStream());
			out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			connect(login, password);
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

	private void connect(String login, String password) throws IOException {
		unpacker.readPayload(ByteBuffer.allocate(64));// greeting
		final byte[] salt = new byte[44];
		unpacker.readPayload(salt);
		unpacker.readPayload(ByteBuffer.allocate(20));// unused
		if (login != null) {
			writeCode(Util.CODE_AUTH);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_USER_NAME);
			packer.packString(login);
			packer.packInt(Util.KEY_TUPLE);
			packer.packArrayHeader(2);
			packer.packString("chap-sha1");
			packer.packBinaryHeader(20);
			packer.addPayload(scramble(password, salt));
			finishQuery();
			// unpacker.unpackByte();
			int bodySize = flushAndGetResultSize(false);
			if (bodySize == 1) {
				byte bodyKey = unpacker.unpackByte();
				if (bodyKey == Util.KEY_ERROR) {
					throw new TarantoolAuthException(unpacker.unpackString());
				} else {
					throw new TarantoolException("Unknown body Key " + bodyKey);
				}
			}
			if (bodySize > 1) {
				throw new TarantoolException("Body size " + bodySize + " for auth");
			}
		}
	}

	private static byte[] scramble(String password, byte[] salt) {
		final MessageDigest sha1;
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new TarantoolException(e);
		}

		byte[] step1 = sha1.digest(password.getBytes());

		sha1.reset();
		byte[] step2 = sha1.digest(step1);
		sha1.reset();
		sha1.update(Base64.getDecoder().decode(salt), 0, 20);
		sha1.update(step2);
		byte[] step3 = sha1.digest();
		for (int i = 0; i < 20; i++) {
			step1[i] ^= step3[i];
		}
		return step1;
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
