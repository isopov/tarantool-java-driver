package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.Nullable;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface Result extends Closeable {

    int getSize();

    boolean hasNext();

    boolean isNull(int index);

    boolean getBoolean(int index);

    double getDouble(int index);

    float getFloat(int index);

    long getLong(int index);

    int getInt(int index);

    @Nullable
    String getString(int index);

    byte[] getBytes(int index);

    ByteBuffer getByteBuffer(int index);

    int currentSize();

    boolean next();

    @Override
    void close();
}
