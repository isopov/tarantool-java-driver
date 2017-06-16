package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
//			assertTrue(client.select(281) > 0);
			assertEquals(61, client.select(281));
		}
	}
	

}
