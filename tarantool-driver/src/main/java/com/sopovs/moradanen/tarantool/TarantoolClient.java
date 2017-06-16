package com.sopovs.moradanen.tarantool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class TarantoolClient implements Closeable {

	private final Socket socket;
	private final DataOutputStream out;
	private final DataInputStream in;

	public TarantoolClient(String host) throws IOException{
		this(host, 3301);
	}
	
	public TarantoolClient(String host, int port) throws IOException {
		this(new Socket(host, port));
	}

	public TarantoolClient(Socket socket) throws IOException {
		this.socket = socket;
		this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
		this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		connect();
	}
	
	private void connect() throws IOException{
		byte[] bytes = new byte[64];
		//hello
		in.readFully(bytes);
		//password salt
		in.readFully(bytes);
	}
	

	public void close() throws IOException {
		out.flush();
		socket.close();
	}

}
