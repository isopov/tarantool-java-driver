package com.sopovs.moradanen.tarantool;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.msgpack.core.MessageUnpacker;

import com.sopovs.moradanen.tarantool.core.TarantoolException;
import com.sopovs.moradanen.tarantool.core.Util;

public class SqlResult extends AbstractResult {
	private final Map<String, Integer> fieldNames = new HashMap<>();
	private final Map<String, Integer> fieldNamesView = Collections.unmodifiableMap(fieldNames);
	private final int size;

	SqlResult(MessageUnpacker unpacker) {
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

	public Map<String, Integer> getFieldNames() {
		return fieldNamesView;
	}

	public int getIndex(String name) {
		Integer index = fieldNames.get(name);
		if (index == null) {
			throw new TarantoolException("No field " + name);
		}
		return index;
	}
}
