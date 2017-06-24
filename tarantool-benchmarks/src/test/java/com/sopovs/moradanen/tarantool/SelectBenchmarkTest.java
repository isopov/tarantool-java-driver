package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelectBenchmarkTest {

	private SelectBenchmark benchmark = new SelectBenchmark();

	@Before
	public void setup() throws Exception {
		benchmark.size=33;
		benchmark.setup();
	}

	@Test
	public void testClient() throws Exception {
		assertEquals(33, benchmark.client().size());
	}
	
	@Test
	public void testReferenceClient() throws Exception {
		assertEquals(33, benchmark.referenceClient().size());
	}
	

	@Test
	public void testConnection() {
		assertEquals(33, benchmark.connection().size());
	}

	@After
	public void tearDown() {
		benchmark.tearDown();
	}
}
