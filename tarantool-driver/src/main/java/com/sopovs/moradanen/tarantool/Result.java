package com.sopovs.moradanen.tarantool;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;

//TODO Closeable?
public class Result {

	private int counter;
	private final int size;
	private final MessageUnpacker unpacker;
	private ImmutableArrayValue current;

	Result(MessageUnpacker unpacker) {
		try {
			this.size = unpacker.unpackArrayHeader();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		this.unpacker = unpacker;
	}

	public int getSize() {
		return size;
	}

	public boolean hasNext() {
		return counter < size;
	}

	public int getInt(int index) {
		return current.get(index).asIntegerValue().asInt();
	}

	public String getString(int index) {
		return current.get(index).asStringValue().asString();
	}

	public int currentSize() {
		return current.size();
	}

	public void next() {
		counter++;
		try {
			current = unpacker.unpackValue().asArrayValue();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	public void consume() {
		while (counter < size) {
			counter++;
			try {
				// TODO seems like it may hang in case no data to read...
				unpacker.unpackValue();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

}
