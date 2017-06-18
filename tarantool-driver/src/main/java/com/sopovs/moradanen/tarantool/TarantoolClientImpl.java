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
	private final MessageBufferPacker packer;
	private final Tuple tuple;
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
		packer = MessagePack.newDefaultBufferPacker();
		tuple = new Tuple(packer);
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
		batchSize++;
	}

	@Override
	public void executeBatch() {
		for (int i = 0; i < batchSize; i++) {
			Result result = execute(true);
			result.consume();
		}
		batchSize = 0;
	}

	@Override
	public void eval(String expression, TupleWriter tupleWriter) {
		try {
			writeCode(Util.CODE_EVAL);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_EXPRESSION);
			packer.packString(expression);
			packer.packInt(Util.KEY_TUPLE);
			tupleWriter.writeTuple(tuple);
			finishQuery();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void select(int space, TupleWriter keyWriter, int index, int limit, int offset) {
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
			packer.packInt(Util.KEY_KEY);
			keyWriter.writeTuple(tuple);
			packer.packInt(Util.KEY_LIMIT);
			packer.packInt(limit);

			packer.packInt(Util.KEY_OFFSET);
			packer.packInt(offset);

			finishQuery();
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
	public void insert(int space, TupleWriter tupleWriter) {
		insertOrReplace(Util.CODE_INSERT, space, tupleWriter);
	}

	@Override
	public void replace(int space, TupleWriter tupleWriter) {
		insertOrReplace(Util.CODE_REPLACE, space, tupleWriter);
	}

	private void insertOrReplace(int code, int space, TupleWriter tupleWriter) {
		try {
			writeCode(code);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			packer.packInt(Util.KEY_TUPLE);
			tupleWriter.writeTuple(tuple);
			finishQuery();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public void delete(int space, TupleWriter keyWriter, int index) {
		try {
			writeCode(Util.CODE_DELETE);
			packer.packMapHeader(3);
			packer.packInt(Util.KEY_SPACE);
			packer.packInt(space);
			packer.packInt(Util.KEY_KEY);
			keyWriter.writeTuple(tuple);
			packer.packInt(Util.KEY_INDEX);
			packer.packInt(index);
			finishQuery();
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

}
