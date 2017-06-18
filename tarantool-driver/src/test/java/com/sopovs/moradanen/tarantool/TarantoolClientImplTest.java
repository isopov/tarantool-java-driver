package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TarantoolClientImplTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testConnect() throws IOException {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
		}
	}

	@Test
	public void testSelect() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				Result result = client.selectAll(Util.SPACE_VSPACE, 10)) {
			assertEquals(10, result.getSize());
		}
	}
	
	@Test
	public void testSelectByName(){
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				Result result = client.selectAll("_vspace", 3)) {
			assertEquals(3, result.getSize());
		}
	}

	@Test
	public void testManySelects() throws IOException {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			for (int i = 1; i <= 10; i++) {
				try (Result result = client.selectAll(Util.SPACE_VSPACE, i)) {
					assertEquals(i, result.getSize());
				}
			}
		}
	}

	@Test
	public void testSpace() throws IOException {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			assertEquals(Util.SPACE_SCHEMA, client.space("_schema"));
			assertEquals(Util.SPACE_SPACE, client.space("_space"));
			assertEquals(Util.SPACE_INDEX, client.space("_index"));
			assertEquals(Util.SPACE_FUNC, client.space("_func"));
			assertEquals(Util.SPACE_VSPACE, client.space("_vspace"));
			assertEquals(Util.SPACE_VINDEX, client.space("_vindex"));
			assertEquals(Util.SPACE_VFUNC, client.space("_vfunc"));
			assertEquals(Util.SPACE_USER, client.space("_user"));
			assertEquals(Util.SPACE_PRIV, client.space("_priv"));
			assertEquals(Util.SPACE_CLUSTER, client.space("_cluster"));
		}
	}

	@Test
	public void testNoSpace() throws IOException {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage("No such space no_such_space");
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			client.space("no_such_space");
		}
	}

	@Test
	public void testEval() throws IOException {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				Result result1 = client.eval("box.schema.space.create('javatest')");
				Result result2 = client
						.eval("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})");
				Result result3 = client.eval("box.space.javatest:drop()")) {
		}
	}

}
