package com.sopovs.moradanen.tarantool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;

import com.sopovs.moradanen.tarantool.core.TarantoolException;

public abstract class AbstractResult implements Result {

	private int counter;
	protected final MessageUnpacker unpacker;
	protected ImmutableArrayValue current;

	AbstractResult(MessageUnpacker unpacker) {
		this.unpacker = unpacker;

	}

	public boolean hasNext() {
		return counter < getSize();
	}

	public void consume() {
		while (counter < getSize()) {
			counter++;
			try {
				// TODO seems like it may hang in case no data to read...
				unpacker.unpackValue();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	@Override
	public boolean next() {
		if (hasNext()) {
			counter++;
			current = nextInternal();
			return true;
		}
		return false;
	}

	@Override
	public boolean isNull(int index) {
		return current.get(index).isNilValue();
	}

	@Override
	public boolean getBoolean(int index) {
		return current.get(index).asBooleanValue().getBoolean();
	}

	@Override
	public double getDouble(int index) {
		return current.get(index).asFloatValue().toDouble();
	}

	@Override
	public float getFloat(int index) {
		return current.get(index).asFloatValue().toFloat();
	}

	@Override
	public long getLong(int index) {
		return current.get(index).asIntegerValue().asLong();
	}

	@Override
	public int getInt(int index) {
		return current.get(index).asIntegerValue().asInt();
	}

	@Override
	public String getString(int index) {
		return current.get(index).asStringValue().asString();
	}

	@Override
	public byte[] getBytes(int index) {
		return current.get(index).asBinaryValue().asByteArray();
	}

	@Override
	public ByteBuffer getByteBuffer(int index) {
		return current.get(index).asBinaryValue().asByteBuffer();
	}

	@Override
	public int currentSize() {
		return current.size();
	}

	private ImmutableArrayValue nextInternal() {
		try {
			return unpacker.unpackValue().asArrayValue();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

}
