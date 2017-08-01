package com.sopovs.moradanen.tarantool;

import java.io.IOException;

import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;

public class ArrayResult extends AbstractResult<ImmutableArrayValue> {
	private final int size;

	ArrayResult(MessageUnpacker unpacker) {
		super(unpacker);
		size = getArraySize(unpacker);
	}

	@Override
	public int getSize() {
		return size;
	}

	private static int getArraySize(MessageUnpacker unpacker) {
		try {
			return unpacker.unpackArrayHeader();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
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
	public int currentSize() {
		return current.size();
	}

	@Override
	protected ImmutableArrayValue nextInternal() {
		try {
			return unpacker.unpackValue().asArrayValue();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

}
