package com.sopovs.moradanen.tarantool.core;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AssertTest {

    @Test
    void testAssert() {
        assertThrows(AssertionError.class, () -> {
            assert false : "Make sure to run tests with asserts enabled";
        });
    }
}
