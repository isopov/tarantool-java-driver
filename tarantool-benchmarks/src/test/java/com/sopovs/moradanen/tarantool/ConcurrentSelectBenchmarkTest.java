package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentSelectBenchmarkTest {

	private ConcurrentSelectBenchmark benchmark = new ConcurrentSelectBenchmark();

	@Before
	public void setup() throws Exception {
		benchmark.size = 33;
	}

	@Test
	public void testClient() throws Exception {
		benchmark.type = "pooledClientSource";
		benchmark.setup();
		assertTrue(benchmark.clientSource().startsWith("FooBar"));
	}

	@Test
	public void testReferenceClient() throws Exception {
		benchmark.type = "referenceClient";
		benchmark.setup();
		assertTrue(benchmark.referenceClient().startsWith("FooBar"));
	}
	
	@Test
	public void testThreadLocal() throws Exception {
		benchmark.type = "threadLocal";
		benchmark.setup();
		assertTrue(benchmark.threadLocal().startsWith("FooBar"));
	}
	

	@After
	public void tearDown() {
		benchmark.tearDown();
	}
}
