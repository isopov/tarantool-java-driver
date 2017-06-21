package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.sopovs.moradanen.tarantool.TarantoolClient.Op;

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

			client.selectAll(Util.SPACE_VSPACE);
			Result result = client.execute();
			assertTrue(result.getSize() > 0);
			result.consume();
			assertFalse(result.hasNext());
		}
	}

	@Test
	public void testSelectByName() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			client.selectAll("_vspace", 3);
			Result result = client.execute();
			assertEquals(3, result.getSize());
			result.consume();
			assertFalse(result.hasNext());
		}
	}

	@Test
	public void testManySelects() {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			for (int i = 1; i <= 10; i++) {
				client.selectAll(Util.SPACE_VSPACE, i);
				Result result = client.execute();
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
			client.evalFully("box.schema.space.create('javatest')");
			client.evalFully("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})");
			client.evalFully("box.space.javatest:drop()");
		}
	}

	@Test
	public void testInsert() throws Exception {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			insertInternal(client);
		}
	}

	@Test
	public void testDelete() throws Exception {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			insertInternal(client);
			client.delete("javatest");
			client.setInt(1);
			Result delete = client.execute();
			assertEquals(1, delete.getSize());
			delete.consume();

			client.select("javatest", 0);
			client.setInt(1);
			Result select = client.execute();
			assertEquals(0, select.getSize());
		}
	}

	private static void insertInternal(TarantoolClientImpl client) {
		createTestSpace(client);

		client.insert("javatest");
		client.setInt(1);
		client.setInt(0);
		client.setString("Foobar");

		Result insert = client.execute();
		assertEquals(1, insert.getSize());
		insert.consume();

		client.selectAll("javatest");
		Result selectAll = client.execute();
		assertEquals(1, selectAll.getSize());
		selectAll.consume();

		client.select("javatest", 0);
		client.setInt(1);
		Result select = client.execute();
		assertEquals(1, select.getSize());
		select.next();
		assertEquals(1, select.getInt(0));
		assertEquals(0, select.getInt(1));
		assertEquals("Foobar", select.getString(2));
	}

	private static void createTestSpace(TarantoolClientImpl client) {
		client.evalFully("box.schema.space.create('javatest')").consume();
		client.evalFully("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})")
				.consume();
	}

	@Test
	public void testInsertBatch() throws Exception {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			createTestSpace(client);
			int space = client.space("javatest");
			for (int i = 0; i < 10; i++) {

				client.insert(space);
				client.setInt(i);
				client.setString("Foo" + i);

				client.addBatch();
			}

			client.executeBatch();

			for (int i = 0; i < 10; i++) {
				client.select(space, 0);
				client.setInt(i);
				Result first = client.execute();
				assertEquals(1, first.getSize());
				first.consume();
			}

		}
	}

	@Test
	public void testUpdate() throws Exception {
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			insertInternal(client);
			client.update(client.space("javatest"), 0);
			client.setInt(1);
			client.change(Op.PLUS, 1, 1);
			Result update = client.execute();
			assertEquals(1, update.getSize());
			update.consume();

			client.select("javatest", 0);
			client.setInt(1);
			Result select = client.execute();
			assertEquals(1, select.getSize());
			select.next();
			assertEquals(1, select.getInt(0));
			assertEquals(1, select.getInt(1));
			assertEquals("Foobar", select.getString(2));
		}
	}

}
