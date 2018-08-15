package com.sopovs.moradanen.tarantool.benchmarks;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SingleSelectBenchmarkTest {

    private final SingleSelectBenchmark benchmark = new SingleSelectBenchmark();

    private static String getEnvTarantoolVersion() {
        String version = System.getenv("TARANTOOL_VERSION");
        return version == null ? "2.0" : version;
    }

    @BeforeEach
    void setup() throws Exception {
        assumeTrue(getEnvTarantoolVersion().startsWith("2.0"));
        benchmark.size = 33;
        benchmark.setup();
    }

    @Test
    void testClient() {
        assertEquals(33, benchmark.client().size());
    }

    @Test
    void testReferenceClient() {
        assertEquals(33, benchmark.referenceClient().size());
    }

    @Test
    void testConnection() {
        assertEquals(33, benchmark.connection().size());
    }

    @Test
    void testJdbc() throws SQLException {
        assertEquals(33, benchmark.jdbc().size());
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (getEnvTarantoolVersion().startsWith("2.0")) {
            benchmark.tearDown();
        }
    }
}
