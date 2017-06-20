package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelectBenchmarkTest {

	private SelectBenchmark benchmark = new SelectBenchmark();

	@Before
	public void setup() throws Exception {
		benchmark.setup();
	}

	@Test
	public void testClient() throws Exception {
		assertEquals(1, benchmark.client());
	}

	@Test
	public void testConnection() {
		assertEquals(1, benchmark.connection().size());
	}

	@After
	public void tearDown() {
		benchmark.tearDown();
	}
}
