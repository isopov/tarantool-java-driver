package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.Nullable;
import com.sopovs.moradanen.tarantool.core.TarantoolException;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

abstract class AbstractResult implements Result {

    private final MessageUnpacker unpacker;
    private int counter;
    @Nullable
    private ImmutableArrayValue current;

    AbstractResult(MessageUnpacker unpacker) {
        this.unpacker = unpacker;

    }

    private static String decode(ValueType valueType) {
        switch (valueType) {
            case NIL:
                return "null";
            case MAP:
            case ARRAY:
            case FLOAT:
            case BINARY:
            case STRING:
            case BOOLEAN:
            case INTEGER:
            case EXTENSION:
                return valueType.toString().toLowerCase();
            default:
                throw new TarantoolException("Unknown msgpack value type " + valueType);
        }
    }

    @Override
    public boolean hasNext() {
        return counter < getSize();
    }

    @Override
    public void close() {
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

    private void checkNextCalled() {
        if (current == null) {
            throw new TarantoolException("next() was not called on result");
        }
    }

    @Override
    public boolean isNull(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        return current.get(index).isNilValue();
    }

    @Override
    public boolean getBoolean(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asBooleanValue().getBoolean();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected boolean, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public double getDouble(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asFloatValue().toDouble();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected float, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public float getFloat(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asFloatValue().toFloat();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected float, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public long getLong(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asIntegerValue().asLong();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected integer, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public int getInt(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asIntegerValue().asInt();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected integer, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    @Nullable
    public String getString(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        Value value = current.get(index);
        if (value.isNilValue()) {
            return null;
        }
        try {
            return value.asStringValue().asString();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected string, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public byte[] getBytes(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asBinaryValue().asByteArray();

        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected binary, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public ByteBuffer getByteBuffer(int index) {
        checkNextCalled();
        assert current != null; //hack to disable warning
        try {
            return current.get(index).asBinaryValue().asByteBuffer();
        } catch (MessageTypeCastException e) {
            throw new TarantoolException("Expected binary, but got " + decode(current.get(index).getValueType()), e);
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public int currentSize() {
        checkNextCalled();
        assert current != null; //hack to disable warning
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
