package com.sopovs.moradanen.tarantool;

import org.junit.Test;

public class AssertTest {

	@Test(expected = AssertionError.class)
	public void testAssert() {
		assert false : "Make sure to run tests with asserts enabled";
	}
}
