package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.TarantoolException;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AbstractResultTest {


    @Test
    void testNextNotCalled() {
        AbstractResult result = new AbstractResult(MessagePack.newDefaultUnpacker(new byte[0])) {
            @Override
            public int getSize() {
                return 0;
            }
        };

        assertThrows(TarantoolException.class, result::currentSize);
        assertThrows(TarantoolException.class, () -> result.isNull(0));
        assertThrows(TarantoolException.class, () -> result.getBoolean(0));
        assertThrows(TarantoolException.class, () -> result.getString(0));
        assertThrows(TarantoolException.class, () -> result.getBytes(0));
        assertThrows(TarantoolException.class, () -> result.getByteBuffer(0));
        assertThrows(TarantoolException.class, () -> result.getInt(0));
        assertThrows(TarantoolException.class, () -> result.getLong(0));
        assertThrows(TarantoolException.class, () -> result.getFloat(0));
        assertThrows(TarantoolException.class, () -> result.getDouble(0));
    }

}