package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TarantoolClientTest {
    @Rule
    public ExpectedException thrown= ExpectedException.none();
	

	@Test
	public void testConnect() throws IOException {
		try (TarantoolClient client = new TarantoolClient("localhost")) {
		}
	}

	@Test
	public void testSelect() throws IOException {
		try (TarantoolClient client = new TarantoolClient("localhost")) {
			assertEquals(10, client.selectAll(281, 10).length);
		}
	}

	@Test
	public void testManySelects() throws IOException {
		try (TarantoolClient client = new TarantoolClient("localhost")) {
			for (int i = 1; i <= 10; i++) {
				assertEquals(i, client.selectAll(281, i).length);
			}
		}
	}
	
	@Test
	public void testSpace() throws IOException {
		try (TarantoolClient client = new TarantoolClient("localhost")) {
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
		thrown.expect(IOException.class);
		thrown.expectMessage("No such space no_such_space");
		try (TarantoolClient client = new TarantoolClient("localhost")) {
			client.space("no_such_space");
		}
	}
	
	@Test
	public void testEval() throws IOException{
		try (TarantoolClient client = new TarantoolClient("localhost")) {
			client.eval("box.schema.space.create('javatest')");
			client.eval("box.space.javatest:drop()");
		}
	}
	
	
}
