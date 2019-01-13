package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.Nullable;

import java.nio.ByteBuffer;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

//TODO Closeable?
public interface Result {

    int getSize();

    boolean hasNext();

    boolean isNull(int index);

    boolean getBoolean(int index);

    double getDouble(int index);

    OptionalDouble getOptionalDouble(int index);

    float getFloat(int index);

    long getLong(int index);

    OptionalLong getOptionalLong(int index);

    int getInt(int index);

    OptionalInt getOptionalInt(int index);

    @Nullable
    String getString(int index);

    byte[] getBytes(int index);

    ByteBuffer getByteBuffer(int index);

    int currentSize();

    boolean next();

    void consume();
}
