package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.TarantoolException;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;

public class ArrayResult extends AbstractResult {
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
}
