package com.sopovs.moradanen.tarantool;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;

import com.sopovs.moradanen.tarantool.core.Iter;
import com.sopovs.moradanen.tarantool.core.Op;
import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.TarantoolAuthException;
import com.sopovs.moradanen.tarantool.core.TarantoolException;
import com.sopovs.moradanen.tarantool.core.Util;

//TODO finalize equivalent via PhantomReference
public class TarantoolClientImpl implements TarantoolClient {
	static final String PRE_CHANGE_EXCEPTION = "Need to call update/upsert before change";
	static final String EXECUTE_ABSENT_EXCEPTION = "Trying to execute absent query";
	static final String PRE_ACTION_EXCEPTION = "Execute or add to batch action before starting next one";
	static final String PRE_SET_EXCEPTION = "Need to call one of update/insert/upsert/delete before setting tuple value";

	private final String version;
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
	private static final byte SQL = 9;

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
		case SQL:
			return Util.KEY_SQL_BIND;
		case 0:
		default:
			throw new TarantoolException(EXECUTE_ABSENT_EXCEPTION);
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
			version = connect(login, password);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private Result getSingleResult() {
		try {
			int bodySize = flushAndGetResultSize(true);
			if (bodySize == 1) {
				byte bodyKey = unpacker.unpackByte();
				if (bodyKey == Util.KEY_DATA) {
					return last = new ArrayResult(unpacker);
				} else if (bodyKey == Util.KEY_ERROR) {
					throw new TarantoolException(unpacker.unpackString());
				} else {
					throw new TarantoolException("Unknown body Key " + bodyKey);
				}
			} else if (bodySize == 2) {
				return new SqlResult(unpacker);
			} else {
				throw new TarantoolException("Body size is " + bodySize);
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

	@Override
	public int[] executeBatchUpdate() {
		int[] result = new int[batchSize];
		for (int i = 0; i < batchSize; i++) {
			result[i] = getUpdateResult();
		}
		batchSize = 0;
		return result;
	}

	@Override
	public int executeUpdate() {
		finishQueryWithArguments();
		return getUpdateResult();
	}

	private int getUpdateResult() {
		try {
			int bodySize = flushAndGetResultSize(true);
			if (1 != bodySize) {
				throw new TarantoolException("Body size is " + bodySize);
			}

			byte bodyKey = unpacker.unpackByte();
			if (bodyKey == Util.KEY_ERROR) {
				throw new TarantoolException(unpacker.unpackString());
			}
			if (bodyKey != Util.KEY_SQL_INFO) {
				throw new TarantoolException("Expected SQL_INFO(" + Util.KEY_SQL_INFO + "), but got " + bodyKey);
			}
			int respBodySize = unpacker.unpackMapHeader();
			if (1 != respBodySize) {
				throw new TarantoolException("Non-select body size is " + bodySize);
			}
			int sqlInfo = unpacker.unpackInt();
			if (sqlInfo != Util.KEY_SQL_ROW_COUNT) {
				throw new TarantoolException("Expected SQL_INFO(" + Util.KEY_SQL_INFO + "), but got " + sqlInfo);
			}
			return unpacker.unpackInt();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
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
			List<MessageBuffer> bufferList = queryPacker.toBufferList();
			for (int i = 0; i < bufferList.size(); i++) {
				MessageBuffer messageBuffer = bufferList.get(i);
				// See MessageBufferPackerBenchmark in tarantool-benchmarks
				packer.addPayload(messageBuffer.array(), messageBuffer.arrayOffset(), messageBuffer.size());
			}
			queryPacker.clear();
		}
		currentQuery = 0;
		querySize = 0;
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
		preActionCheck();
		currentQuery = EVAL;
		try {
			writeCode(Util.CODE_EVAL);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_EXPRESSION);
			packer.packString(expression);
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
		preActionCheck();
		currentQuery = SELECT;
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
		packer.flush();
		List<MessageBuffer> bufferList = packer.toBufferList();
		writeSize(bufferList);
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			// See MessageBufferPackerBenchmark in tarantool-benchmarks
			out.write(messageBuffer.array(), messageBuffer.arrayOffset(), messageBuffer.size());
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

	private String parseGreeting() throws IOException {
		byte[] bytes = new byte[64];
		unpacker.readPayload(bytes);
		String greeting = new String(bytes, StandardCharsets.US_ASCII);
		String[] parts = greeting.split(" ");
		if (parts.length < 2 || !"Tarantool".equals(parts[0])) {
			throw new TarantoolException("Unexpected greeting " + greeting);
		}
		return parts[1];
	}

	private String connect(String login, String password) throws IOException {
		String version = parseGreeting();
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
		return version;
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
	public boolean isClosed() {
		return socket.isClosed();
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
		preActionCheck();
		currentQuery = INSERT;
		try {
			writeCode(code);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private void preActionCheck() {
		if (currentQuery != 0) {
			throw new TarantoolException(PRE_ACTION_EXCEPTION);
		}
	}

	@Override
	public void delete(int space, int index) {
		preActionCheck();
		currentQuery = DELETE;
		try {
			writeCode(Util.CODE_DELETE);
			packer.packMapHeader(3);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
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

	private void preSetCheck() {
		if (currentQuery == 0) {
			throw new TarantoolException(PRE_SET_EXCEPTION);
		}
	}

	@Override
	public void setNull() {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packNil();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setBytes(byte[] bytes) {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packBinaryHeader(bytes.length);
			queryPacker.writePayload(bytes);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setLong(long val) {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packLong(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setBoolean(boolean val) {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packBoolean(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setDouble(double val) {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packDouble(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}

	}

	@Override
	public void setFloat(float val) {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packFloat(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void setInt(int val) {
		preSetCheck();
		try {
			querySize++;
			queryPacker.packInt(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}

	}

	@Override
	public void setString(String val) {
		preSetCheck();
		try {
			querySize++;
			if (val == null) {
				queryPacker.packNil();
			} else {
				queryPacker.packString(val);
			}
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void update(int space, int index) {
		preActionCheck();
		currentQuery = UPDATE_KEY;
		try {
			writeCode(Util.CODE_UPDATE);
			packer.packMapHeader(4);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void upsert(int space) {
		preActionCheck();
		currentQuery = UPSERT_TUPLE;
		try {
			writeCode(Util.CODE_UPSERT);
			packer.packMapHeader(3);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private void preChange(String op, int field) throws IOException {
		if (currentQuery == UPDATE_KEY) {
			writeQuery();
			currentQuery = UPDATE_TUPLE;
		} else if (currentQuery == UPSERT_TUPLE) {
			writeQuery();
			currentQuery = UPSERT_OPS;
		} else if (currentQuery != UPDATE_TUPLE && currentQuery != UPSERT_OPS) {
			throw new TarantoolException(PRE_CHANGE_EXCEPTION);
		}
		querySize++;
		queryPacker.packArrayHeader(3);
		queryPacker.packString(op);
		queryPacker.packInt(field);
	}

	@Override
	public void change(IntOp op, int field, int arg) {
		try {
			preChange(op.getVal(), field);
			queryPacker.packInt(arg);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void change(IntOp op, int field, long arg) {
		try {
			preChange(op.getVal(), field);
			queryPacker.packLong(arg);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void change(Op op, int field, String arg) {
		try {
			preChange(op.getVal(), field);
			if (arg == null) {
				queryPacker.packNil();
			} else {
				queryPacker.packString(arg);
			}
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void change(Op op, int field, byte[] bytes) {
		try {
			preChange(op.getVal(), field);
			queryPacker.packBinaryHeader(bytes.length);
			queryPacker.writePayload(bytes);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void sql(String sqlQuery) {
		preActionCheck();
		currentQuery = SQL;

		try {
			writeCode(Util.CODE_EXECUTE);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_SQL_TEXT);
			packer.packString(sqlQuery);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}

	}

	@Override
	public String getVersion() {
		return version;
	}

}
