package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentSelectBenchmarkTest {

	private ConcurrentSelectBenchmark benchmark = new ConcurrentSelectBenchmark();

	@Before
	public void setup() throws Exception {
		benchmark.size = 33;
		benchmark.setup();
	}

	@Test
	public void testClient() throws Exception {
		assertTrue(benchmark.client().startsWith("FooBar"));
	}

	@Test
	public void testReferenceClient() throws Exception {
		Object foo = benchmark.referenceClient();
		assertNotNull(foo);
	}

	@After
	public void tearDown() {
		benchmark.tearDown();
	}
}
