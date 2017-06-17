package com.sopovs.moradanen.tarantool;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;

public class TarantoolClient implements Closeable {
	private static final byte[] EMPTY_FIVE_BYTES = new byte[5];

	private final Socket socket;
	private final MessageUnpacker unpacker;
	private final MessageBufferPacker packer;
	private final DataOutputStream out;
	private int counter;

	public TarantoolClient(String host) throws IOException {
		this(host, 3301);
	}

	public TarantoolClient(String host, int port) throws IOException {
		this(new Socket(host, port));
	}

	public TarantoolClient(Socket socket) throws IOException {
		this.socket = socket;
		packer = MessagePack.newDefaultBufferPacker();
		unpacker = MessagePack.newDefaultUnpacker(socket.getInputStream());
		out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		connect();
	}

	public ImmutableValue[] selectAll(int space) throws IOException {
		return selectAll(space, Integer.MAX_VALUE);
	}

	public ImmutableValue[] selectAll(int space, int limit) throws IOException {
		return selectAll(space, 0, limit, 0);
	}

	public int space(String space) throws IOException {

		ImmutableValue[] result = selectByString(Util.SPACE_VSPACE, space, Util.INDEX_SPACE_NAME);
		if (result.length == 0) {
			throw new IOException("No such space " + space);
		}
		if (result.length != 1) {
			throw new IOException("Unexpected result length " + result.length);
		}
		return result[0].asArrayValue().list().get(0).asIntegerValue().asInt();
	}

	public ImmutableValue[] selectByString(int space, String key, int index) throws IOException {
		return selectByString(space, key, index, Integer.MAX_VALUE, 0);
	}

	public ImmutableValue[] selectByString(int space, String key, int index, int limit, int offset) throws IOException {
		selectStart(space, index);
		packer.packInt(Util.KEY_KEY);
		packer.packArrayHeader(1);
		packer.packString(key);
		return selectEnd(limit, offset);
	}

	public ImmutableValue[] selectAll(int space, int index, int limit, int offset) throws IOException {
		selectStart(space, index);
		packer.packInt(Util.KEY_KEY);
		packer.packArrayHeader(0);
		return selectEnd(limit, offset);
	}

	private ImmutableValue[] selectEnd(int limit, int offset) throws IOException {
		packer.packInt(Util.KEY_LIMIT);
		packer.packInt(limit);

		packer.packInt(Util.KEY_OFFSET);
		packer.packInt(offset);

		writePacket();
		unpacker.unpackInt();
		unpackHeader();
		int bodySize = unpacker.unpackMapHeader();
		if (1 != bodySize) {
			// TODO Other exception?
			throw new IOException("Body size is " + bodySize);
		}

		byte bodyKey = unpacker.unpackByte();
		if (bodyKey == Util.KEY_DATA) {
			ImmutableValue[] result = new ImmutableValue[unpacker.unpackArrayHeader()];
			for (int i = 0; i < result.length; i++) {
				result[i] = unpacker.unpackValue();
			}
			return result;
		} else if (bodyKey == Util.KEY_ERROR) {
			// TODO Other exception?
			throw new IOException(unpacker.unpackString());
		} else {
			throw new IOException("Unknown body Key " + bodyKey);
		}
	}

	private void selectStart(int space, int index) throws IOException {
		packer.packMapHeader(2);
		packer.packInt(Util.KEY_CODE);
		packer.packInt(Util.CODE_SELECT);
		packer.packInt(Util.KEY_SYNC);
		packer.packInt(++counter);

		packer.packMapHeader(6);
		packer.packInt(Util.KEY_SPACE);
		packer.packInt(space);
		packer.packInt(Util.KEY_INDEX);
		packer.packInt(index);

		// TODO
		packer.packInt(Util.KEY_ITERATOR);
		packer.packInt(0);
	}

	private void writePacket() throws IOException {
		// TODO eliminate bytes copy for perf
		byte[] bytes = packer.toByteArray();
		packer.clear();
		out.writeByte(MessagePack.Code.UINT32);
		out.writeInt(bytes.length);
		out.write(bytes);
		out.flush();
	}

	private void unpackHeader() throws IOException {
		int headerSize = unpacker.unpackMapHeader();
		for (int i = 0; i < headerSize; i++) {
			// TODO what to do with the header?
			unpacker.unpackByte();
			unpacker.unpackInt();
		}
	}

	private void connect() throws IOException {
		unpacker.readPayload(ByteBuffer.allocate(128));
	}

	public void close() throws IOException {
		socket.close();
	}

}
