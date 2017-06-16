package com.sopovs.moradanen.tarantool;

import java.io.IOException;

import org.junit.Test;

public class TarantoolClientTest {

	@Test
	public void testConnect() throws IOException {
		try (TarantoolClient client = new TarantoolClient("localhost")) {
		}
	}

}
