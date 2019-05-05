package com.sopovs.moradanen.tarantool.benchmarks;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentSelectBenchmarkTest {

    private final ConcurrentSelectBenchmark benchmark = new ConcurrentSelectBenchmark();

    @BeforeEach
    void setup() {
        benchmark.size = 33;
    }

    @Test
    void testClient() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.POOLED_CLIENT_SOURCE;
        benchmark.setup();
        assertTrue(benchmark.clientSource().startsWith("FooBar"));
    }

    @Test
    @Disabled
    void testReplicatedClient() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.POOLED_REPL_CLIENT_SOURCE;
        benchmark.setup();
        assertTrue(benchmark.clientSource().startsWith("FooBar"));
    }


    @Test
    void testReferenceClient() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.UPSTREAM_CLIENT;
        benchmark.setup();
        assertTrue(benchmark.referenceClient().startsWith("FooBar"));
    }

    @Test
    void testThreadLocal() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.THREAD_LOCAL;
        benchmark.setup();
        assertTrue(benchmark.threadLocal().startsWith("FooBar"));
    }


    @Test
    void testClientAll() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.POOLED_CLIENT_SOURCE;
        benchmark.setup();
        assertEquals(expectedAll(), benchmark.clientSourceAll());
    }

    @Test
    @Disabled
    void testReplicatedClientAll() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.POOLED_REPL_CLIENT_SOURCE;
        benchmark.setup();
        assertEquals(expectedAll(), benchmark.clientSourceAll());
    }

    @Test
    void testUpstreamClientAll() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.UPSTREAM_CLIENT;
        benchmark.setup();
        assertEquals(expectedAll(), benchmark.referenceClientAll());
    }

    @Test
    void testThreadLocalAll() throws Exception {
        benchmark.type = ConcurrentSelectBenchmark.THREAD_LOCAL;
        benchmark.setup();
        assertEquals(expectedAll(), benchmark.threadLocalAll());
    }

    private static List<String> expectedAll() {
        List<String> result = new ArrayList<>(33);
        for (int i = 0; i < 33; i++) {
            result.add("FooBar" + i);
        }
        return result;
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        benchmark.tearDown();
    }
}
