package com.sopovs.moradanen.tarantool.core;

public enum Iter {
    EQ(0), REQ(1), ALL(2), LT(3), LE(4), GE(5), GT(6), BITS_ALL_SET(7), BITS_ANY_SET(8), BITS_ALL_NOT_SET(9), OVERLAPS(
            10), NEIGHBOR(11);
    private final int value;

    Iter(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
