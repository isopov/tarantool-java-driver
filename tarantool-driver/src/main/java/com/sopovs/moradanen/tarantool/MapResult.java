package com.sopovs.moradanen.tarantool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;

public class MapResult extends AbstractResult<ImmutableArrayValue> {
	private final Map<String, Integer> fieldNames = new HashMap<>();
	private final Map<String, Integer> fieldNamesView = Collections.unmodifiableMap(fieldNames);
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
		int metadata = unpacker.unpackInt();
		if (metadata != Util.KEY_METADATA) {
			throw new TarantoolException("Expected METADATA(" + Util.KEY_METADATA + "), but got " + metadata);
		}

		int metadataSize = unpacker.unpackArrayHeader();
		for (int i = 0; i < metadataSize; i++) {
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

	public boolean isNull(String name) {
		return isNull(getIndex(name));
	}

	public boolean getBoolean(String name) {
		return getBoolean(getIndex(name));
	}

	public double getDouble(String name) {
		return getDouble(getIndex(name));
	}

	public float getFloat(String name) {
		return getFloat(getIndex(name));
	}

	public long getLong(String name) {
		return getLong(getIndex(name));
	}

	public int getInt(String name) {
		return getInt(getIndex(name));
	}

	public Map<String, Integer> getFieldNames() {
		return fieldNamesView;
	}

	public String getString(String name) {
		return getString(getIndex(name));
	}

	private int getIndex(String name) {
		Integer index = fieldNames.get(name);
		if (index == null) {
			throw new TarantoolException("No field " + name);
		}
		return index;
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
