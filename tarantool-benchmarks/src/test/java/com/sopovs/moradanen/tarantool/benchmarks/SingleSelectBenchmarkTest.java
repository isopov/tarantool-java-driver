package com.sopovs.moradanen.tarantool.benchmarks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class SingleSelectBenchmarkTest {

    private final SingleSelectBenchmark benchmark = new SingleSelectBenchmark();

    private static String getEnvTarantoolVersion() {
        String version = System.getenv("TARANTOOL_VERSION");
        return version == null ? "2.0" : version;
    }

    @Before
    public void setup() throws Exception {
        assumeTrue(getEnvTarantoolVersion().startsWith("2.0"));
        benchmark.size = 33;
        benchmark.setup();
    }

    @Test
    public void testClient() {
        assertEquals(33, benchmark.client().size());
    }

    @Test
    public void testReferenceClient() {
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
        if (getEnvTarantoolVersion().startsWith("2.0")) {
            benchmark.tearDown();
        }
    }
}
