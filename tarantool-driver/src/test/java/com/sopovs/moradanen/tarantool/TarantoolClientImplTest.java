package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TarantoolClientImplTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testConnect() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
		}
	}

	@Test
	public void testPing() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			client.ping();
		}
	}

	@Test
	public void testSelect() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			Result result = client.selectAll(Util.SPACE_VSPACE, 10);
			assertEquals(10, result.getSize());
			result.consume();
			assertFalse(result.hasNext());
		}
	}

	@Test
	public void testSelectByName() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			Result result = client.selectAll("_vspace", 3);
			assertEquals(3, result.getSize());
			result.consume();
			assertFalse(result.hasNext());
		}
	}

	@Test
	public void testManySelects() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			for (int i = 1; i <= 10; i++) {
				Result result = client.selectAll(Util.SPACE_VSPACE, i);
				assertEquals(i, result.getSize());
				result.consume();
				assertFalse(result.hasNext());

			}
		}
	}

	@Test
	public void testSpace() {
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
	public void testNoSpace() {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage("No such space no_such_space");
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			client.space("no_such_space");
		}
	}

	@Test
	public void testEval() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			client.eval("box.schema.space.create('javatest')");
			client.eval("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})");
			client.eval("box.space.javatest:drop()");
		}
	}

	@Test
	public void testInsert() throws Exception {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.eval("box.space.javatest:drop()")) {
			insertInternal(client);
		}
	}

	@Test
	public void testDelete() throws Exception {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.eval("box.space.javatest:drop()")) {
			insertInternal(client);
			Result update = client.delete("javatest", TupleWriter.integer(1));
			assertEquals(1, update.getSize());
			update.consume();

			Result select = client.select("javatest", 1, 0);
			assertEquals(0, select.getSize());
		}
	}

	private static void insertInternal(TarantoolClientImpl client) {
		client.eval("box.schema.space.create('javatest')");
		client.eval("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})");

		Result insert = client.insert("javatest", tuple -> {
			tuple.writeSize(2);
			tuple.writeInt(1);
			tuple.writeString("Foobar");
		});
		assertEquals(1, insert.getSize());
		insert.consume();

		Result select = client.select("javatest", 1, 0);
		assertEquals(1, select.getSize());
		select.next();
		assertEquals(1, select.getInt(0));
		assertEquals("Foobar", select.getString(1));
	}

}
