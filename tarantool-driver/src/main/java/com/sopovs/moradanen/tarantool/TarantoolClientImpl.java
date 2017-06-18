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

	@Override
	public Result eval(String expression, TupleWriter tupleWriter) {
		try {
			writeCode(Util.CODE_EVAL);
			packer.packMapHeader(2);
			packer.packInt(Util.KEY_EXPRESSION);
			packer.packString(expression);
			packer.packInt(Util.KEY_TUPLE);
			tupleWriter.writeTuple(tuple);
			writePacket();
			return readResponce();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public Result select(int space, String key, int index, int limit, int offset) {
		try {
			selectStart(space, index);
			packer.packInt(Util.KEY_KEY);
			packer.packArrayHeader(1);
			packer.packString(key);
			return selectEnd(limit, offset);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public Result select(int space, int key, int index, int limit, int offset) {
		try {
			selectStart(space, index);
			packer.packInt(Util.KEY_KEY);
			packer.packArrayHeader(1);
			packer.packInt(key);
			return selectEnd(limit, offset);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public Result selectAll(int space, int limit, int offset) {
		try {
			selectStart(space, 0);
			packer.packInt(Util.KEY_KEY);
			packer.packArrayHeader(0);
			return selectEnd(limit, offset);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private Result selectEnd(int limit, int offset) throws IOException {
		packer.packInt(Util.KEY_LIMIT);
		packer.packInt(limit);

		packer.packInt(Util.KEY_OFFSET);
		packer.packInt(offset);

		writePacket();
		return readResponce();
	}

	private Result readResponce() throws IOException {
		unpacker.unpackInt();
		unpackHeader();
		int bodySize = unpacker.unpackMapHeader();
		if (1 != bodySize) {
			throw new TarantoolException("Body size is " + bodySize);
		}

		byte bodyKey = unpacker.unpackByte();
		if (bodyKey == Util.KEY_DATA) {
			return new Result(unpacker);
		} else if (bodyKey == Util.KEY_ERROR) {
			throw new TarantoolException(unpacker.unpackString());
		} else {
			throw new TarantoolException("Unknown body Key " + bodyKey);
		}
	}

	private void selectStart(int space, int index) throws IOException {
		writeCode(Util.CODE_SELECT);

		packer.packMapHeader(6);
		packer.packInt(Util.KEY_SPACE);
		packer.packInt(space);
		packer.packInt(Util.KEY_INDEX);
		packer.packInt(index);

		// TODO
		packer.packInt(Util.KEY_ITERATOR);
		packer.packInt(0);
	}

	private void writeCode(int code) throws IOException {
		packer.packMapHeader(2);
		packer.packInt(Util.KEY_CODE);
		packer.packInt(code);
		packer.packInt(Util.KEY_SYNC);
		packer.packInt(++counter);
	}

	private void writePacket() throws IOException {
		List<MessageBuffer> bufferList = packer.toBufferList();
		writeSize(bufferList);
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			out.write(messageBuffer.toByteArray());
		}
		packer.clear();
		out.flush();
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

	private void unpackHeader() throws IOException {
		int headerSize = unpacker.unpackMapHeader();
		for (int i = 0; i < headerSize; i++) {
			byte key = unpacker.unpackByte();
			if (key == Util.KEY_SYNC) {
				int sync = unpacker.unpackInt();
				if (sync != counter) {
					throw new IOException("Expected responce to " + counter + " and came to " + sync);
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

}
