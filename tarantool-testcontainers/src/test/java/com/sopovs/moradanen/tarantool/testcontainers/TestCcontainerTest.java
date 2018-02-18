package com.sopovs.moradanen.tarantool.testcontainers;

import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientImpl;

public class TestCcontainerTest {

	@Rule
	public GenericContainer tarantool = new GenericContainer<>("tarantool/tarantool:1.7").withExposedPorts(3301);

	@Test
	public void testContainer() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost", tarantool.getMappedPort(3301))) {
		}
	}

}
