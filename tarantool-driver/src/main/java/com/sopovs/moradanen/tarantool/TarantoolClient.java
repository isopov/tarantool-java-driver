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
		// packer = MessagePack.newDefaultPacker(socket.getOutputStream());
		unpacker = MessagePack.newDefaultUnpacker(socket.getInputStream());
		out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		connect();
	}

	public Object[] select(int space, int limit) throws IOException {
		packer.addPayload(EMPTY_FIVE_BYTES);

		packer.packMapHeader(2);
		packer.packInt(Util.KEY_CODE);
		packer.packInt(Util.CODE_SELECT);
		packer.packInt(Util.KEY_SYNC);
		packer.packInt(++counter);

		packer.packMapHeader(6);
		packer.packInt(Util.KEY_SPACE);
		packer.packInt(space);

		// TODO
		packer.packInt(Util.KEY_INDEX);
		packer.packInt(0);

		packer.packInt(Util.KEY_KEY);
		packer.packArrayHeader(0);

		packer.packInt(Util.KEY_ITERATOR);
		packer.packInt(0);

		packer.packInt(Util.KEY_LIMIT);
		packer.packInt(limit);

		// TODO
		packer.packInt(Util.KEY_OFFSET);
		packer.packInt(0);

		byte[] bytes = packer.toByteArray();
		packer.clear();
		writeSize(bytes);
		out.write(bytes);
		out.flush();

		unpacker.unpackInt();
		unpackHeader();
		int bodySize = unpacker.unpackMapHeader();
		if (1 != bodySize) {
			// TODO Other exception?
			throw new IOException("Body size is " + bodySize);
		}

		byte bodyKey = unpacker.unpackByte();
		if (bodyKey == Util.KEY_DATA) {
			Object[] result = new Object[unpacker.unpackArrayHeader()];
			for (int i = 0; i < result.length; i++) {
				result[i] = unpacker.unpackValue();	
			}
			return result;
		} else if (bodyKey == Util.KEY_ERROR) {
			// TODO Other exception?
			throw new IOException(unpacker.unpackString());
		}else{
			throw new IOException("Unknown body Key " + bodyKey);
		}

	}

	private void unpackHeader() throws IOException {
		int headerSize = unpacker.unpackMapHeader();
		for (int i = 0; i < headerSize; i++) {
			// TODO what to do with the header?
			unpacker.unpackByte();
			unpacker.unpackInt();
		}
	}

	private static void writeSize(byte[] bytes) {
		bytes[0] = MessagePack.Code.UINT32;
		// TODO do without allocation
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(bytes.length - 5);
		byte[] size = buffer.array();
		bytes[1] = size[0];
		bytes[2] = size[1];
		bytes[3] = size[2];
		bytes[4] = size[3];
	}

	private void connect() throws IOException {
		unpacker.readPayload(ByteBuffer.allocate(128));
	}

	public void close() throws IOException {
		socket.close();
	}

}
