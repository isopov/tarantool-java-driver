package com.sopovs.moradanen.tarantool;

import java.io.IOException;

import org.msgpack.core.MessagePacker;

public class Tuple {

	private final MessagePacker packer;

	Tuple(MessagePacker packer) {
		this.packer = packer;
	}
	
	public void writeSize(int size){
		try {
			packer.packArrayHeader(size);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}
	
	
	public void writeInt(int val){
		try {
			packer.packInt(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}
	
	public void writeString(String val){
		try {
			packer.packString(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}
	
	public void writeLong(long val){
		try {
			packer.packLong(val);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}
}
