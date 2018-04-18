package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.TarantoolException;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public abstract class AbstractResult implements Result {

    private int counter;
    private final MessageUnpacker unpacker;
    private ImmutableArrayValue current;

    AbstractResult(MessageUnpacker unpacker) {
        this.unpacker = unpacker;

    }

    @Override
    public boolean hasNext() {
        return counter < getSize();
    }

    @Override
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
        try {
            return current.get(index).asBooleanValue().getBoolean();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public double getDouble(int index) {
        try {
            return current.get(index).asFloatValue().toDouble();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public float getFloat(int index) {
        try {
            return current.get(index).asFloatValue().toFloat();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public long getLong(int index) {
        try {
            return current.get(index).asIntegerValue().asLong();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public int getInt(int index) {
        try {
            return current.get(index).asIntegerValue().asInt();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public String getString(int index) {
        Value value = current.get(index);
        if (value.isNilValue()) {
            return null;
        }
        try {
            return value.asStringValue().asString();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public byte[] getBytes(int index) {
        try {
            return current.get(index).asBinaryValue().asByteArray();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
    }

    @Override
    public ByteBuffer getByteBuffer(int index) {
        try {
            return current.get(index).asBinaryValue().asByteBuffer();
        } catch (MessagePackException e) {
            throw new TarantoolException(e);
        }
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
