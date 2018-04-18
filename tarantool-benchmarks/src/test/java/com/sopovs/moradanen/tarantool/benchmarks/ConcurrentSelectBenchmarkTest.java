package com.sopovs.moradanen.tarantool.benchmarks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcurrentSelectBenchmarkTest {

    private final ConcurrentSelectBenchmark benchmark = new ConcurrentSelectBenchmark();

    @Before
    public void setup() {
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


    @Test
    public void testClientAll() throws Exception {
        benchmark.type = "pooledClientSource";
        benchmark.setup();
        assertEquals(expectedAll(), benchmark.clientSourceAll());
    }

    @Test
    public void testReferenceClientAll() throws Exception {
        benchmark.type = "referenceClient";
        benchmark.setup();
        assertEquals(expectedAll(), benchmark.referenceClientAll());
    }

    @Test
    public void testThreadLocalAll() throws Exception {
        benchmark.type = "threadLocal";
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

    @After
    public void tearDown() {
        benchmark.tearDown();
    }
}
