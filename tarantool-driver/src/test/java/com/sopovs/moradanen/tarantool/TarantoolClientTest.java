package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class TarantoolClientTest {

	@Test
	public void testConnect() throws IOException {
		try (TarantoolClient client = new TarantoolClient("localhost")) {
		}
	}
	
	@Test
	public void testSelect() throws IOException{
		try (TarantoolClient client = new TarantoolClient("localhost")) {
			assertEquals(10, client.select(281, 10).length);
		}
	}
	
	@Test
	public void testManySelects() throws IOException{
		try (TarantoolClient client = new TarantoolClient("localhost")) {
			for (int i = 1; i <= 10; i++) {
				assertEquals(i, client.select(281, i).length);
			}
		}
	}

}
