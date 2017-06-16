package com.sopovs.moradanen.tarantool;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

public class TarantoolClient implements Closeable {
	private static final byte[] EMPTY_FIVE_BYTES = new byte[5];

	private final Socket socket;
	private final MessageUnpacker unpacker;
	private final MessagePacker packer;
	private int counter;

	public TarantoolClient(String host) throws IOException {
		this(host, 3301);
	}

	public TarantoolClient(String host, int port) throws IOException {
		this(new Socket(host, port));
	}

	public TarantoolClient(Socket socket) throws IOException {
		this.socket = socket;
		packer = MessagePack.newDefaultPacker(socket.getOutputStream());
		unpacker = MessagePack.newDefaultUnpacker(socket.getInputStream());
		connect();
	}

	public int select(int space) throws IOException {
//		packer.addPayload(EMPTY_FIVE_BYTES);

		packer.packMapHeader(2);
		packer.packInt(Util.KEY_CODE);
		packer.packInt(Util.CODE_SELECT);
		packer.packInt(Util.KEY_SYNC);
		packer.packInt(++counter);
		
		packer.packMapHeader(6);
		packer.packInt(Util.KEY_SPACE);
		packer.packInt(space);
		
		//TODO
		packer.packInt(Util.KEY_INDEX);
		packer.packInt(0);
		
		packer.packInt(Util.KEY_KEY);
		packer.packArrayHeader(0);
		
		packer.packInt(Util.KEY_ITERATOR);
		packer.packInt(0);
		
		//TODO
		packer.packInt(Util.KEY_LIMIT);
		packer.packInt(10);

		//TODO
		packer.packInt(Util.KEY_OFFSET);
		packer.packInt(0);
		
		packer.flush();
		
		return unpacker.unpackInt();
	}

	private void connect() throws IOException {
		unpacker.readPayload(ByteBuffer.allocate(128));
	}

	public void close() throws IOException {
		socket.close();
	}

}
