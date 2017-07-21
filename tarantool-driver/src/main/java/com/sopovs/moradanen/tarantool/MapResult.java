package com.sopovs.moradanen.tarantool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;

public class MapResult extends AbstractResult<ImmutableArrayValue> {
	private final Map<String, Integer> fieldNames = new HashMap<>();
	private final int size;

	MapResult(MessageUnpacker unpacker) {
		super(unpacker);
		try {
			size = getSize(unpacker);
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	@Override
	public int getSize() {
		return size;
	}

	private int getSize(MessageUnpacker unpacker) throws IOException {
		int sqlBodySize = unpacker.unpackMapHeader();
		if (sqlBodySize != 2) {
			throw new TarantoolException("Execute select response body size is " + sqlBodySize);
		}

		int metadata = unpacker.unpackInt();
		if (metadata != Util.KEY_METADATA) {
			throw new TarantoolException("Expected METADATA(" + Util.KEY_METADATA + "), but got " + metadata);
		}

		for (int i = 0; i < unpacker.unpackArrayHeader(); i++) {
			int fieldMetadataSize = unpacker.unpackMapHeader();
			if (fieldMetadataSize != 1) {
				throw new TarantoolException("Field metadata size is " + fieldMetadataSize);
			}
			int fieldName = unpacker.unpackInt();
			if (fieldName != Util.KEY_FIELD_NAME) {
				throw new TarantoolException("Expected FIELD_NAME(" + Util.KEY_FIELD_NAME + "), but got " + fieldName);
			}
			fieldNames.put(unpacker.unpackString(), i);
		}
		int data = unpacker.unpackInt();
		if (data != Util.KEY_DATA) {
			throw new TarantoolException("Expected DATA(" + Util.KEY_DATA + "), but got " + data);
		}

		return unpacker.unpackArrayHeader();
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
	public double getFloat(int index) {
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
