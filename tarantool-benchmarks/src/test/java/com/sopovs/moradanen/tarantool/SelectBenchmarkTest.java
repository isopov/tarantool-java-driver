package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelectBenchmarkTest {

	private SelectBenchmark benchmark = new SelectBenchmark();

	private static String getEnvTarantoolVersion() {
		String minor = System.getenv("TARANTOOL_VERSION");
		minor = minor == null ? "8" : minor;
		String majorMinor = "1." + minor;
		return majorMinor;
	}

	@Before
	public void setup() throws Exception {
		assumeTrue(getEnvTarantoolVersion().startsWith("1.8"));
		benchmark.size = 33;
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

	@Test
	public void testJdbc() throws SQLException {
		assertEquals(33, benchmark.jdbc().size());
	}

	@After
	public void tearDown() throws SQLException {
		if (getEnvTarantoolVersion().startsWith("1.8")) {
			benchmark.tearDown();
		}
	}
}
