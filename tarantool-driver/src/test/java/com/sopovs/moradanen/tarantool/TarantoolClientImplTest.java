package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Assume;
import org.junit.Test;

public class TarantoolClientImplTest {
	@Test
	public void testConnect() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
		}
	}

	@Test
	public void testAuthSuccess() {
		testAuth("foobar", "foobar");
	}

	public static void testAuth(String login, String password) {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.evalFully("box.schema.user.drop('foobar',{if_exists=true})").consume();
			client.evalFully("box.schema.user.create('foobar', {password = 'foobar'})").consume();
			try (TarantoolClientImpl authClient = new TarantoolClientImpl("localhost", 3301, login, password)) {
				selectInternal(authClient);
			}
			client.evalFully("box.schema.user.drop('foobar')").consume();
		}
	}

	@Test
	public void testPing() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.ping();
		}
	}

	@Test
	public void testSelect() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			selectInternal(client);
		}
	}

	private static void selectInternal(TarantoolClient client) {
		client.selectAll(Util.SPACE_VSPACE);
		Result result = client.execute();
		assertTrue(result.getSize() > 0);
		result.consume();
		assertFalse(result.hasNext());
	}

	@Test
	public void testSelectByName() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.selectAll("_vspace", 3);
			Result result = client.execute();
			assertEquals(3, result.getSize());
			result.consume();
			assertFalse(result.hasNext());
		}
	}

	@Test
	public void testManySelects() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
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
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
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
	public void testEval() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.evalFully("box.schema.space.create('javatest')");
			client.evalFully("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'num'}})");
			client.evalFully("box.space.javatest:drop()");
		}
	}

	@Test
	public void testInsert() throws Exception {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			insertInternal(client);
		}
	}

	@Test
	public void testDelete() throws Exception {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
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

	private static void insertInternal(TarantoolClient client) {
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

	private static void createTestSpace(TarantoolClient client) {
		client.evalFully("box.schema.space.create('javatest')").consume();
		client.evalFully("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'num'}})").consume();
	}

	@Test
	public void testInsertBatch() throws Exception {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
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
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			insertInternal(client);
			client.update(client.space("javatest"), 0);
			client.setInt(1);
			client.change(Op.PLUS, 1, 1);
			Result update = client.execute();
			assertEquals(1, update.getSize());

			update.consume();

			testValue(client, 1);
		}
	}

	@Test
	public void testUpsert() throws Exception {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			createTestSpace(client);

			testUpsertStep(client, 1);
			testUpsertStep(client, 2);
			testUpsertStep(client, 3);
		}
	}

	@Test
	public void testUpsertBatch() throws Exception {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				AutoCloseable dropSpace = () -> client.evalFully("box.space.javatest:drop()").consume()) {
			createTestSpace(client);
			for (int i = 0; i < 10; i++) {
				testUpsertBatchStep(client);
				client.addBatch();
			}
			client.executeBatch();
			testValue(client, 10);
		}
	}

	public void testUpsertStep(TarantoolClient client, int value) {
		testUpsertBatchStep(client);
		Result upsert = client.execute();
		assertEquals(0, upsert.getSize());
		upsert.consume();

		testValue(client, value);
	}

	private void testUpsertBatchStep(TarantoolClient client) {
		client.upsert(client.space("javatest"));
		client.setInt(1);
		client.setInt(1);
		client.setString("Foobar");
		client.change(Op.PLUS, 1, 1);
	}

	private void testValue(TarantoolClient client, int value) {
		client.select("javatest", 0);
		client.setInt(1);
		Result select = client.execute();
		assertEquals(1, select.getSize());
		select.next();
		assertEquals(1, select.getInt(0));
		assertEquals(value, select.getInt(1));
		assertEquals("Foobar", select.getString(2));
	}

	@Test
	public void testGetVersion() {
		String majorMinor = getEnvTarantoolVersion();
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			assertTrue(client.getVersion().startsWith(majorMinor));
		}
	}

	private static String getEnvTarantoolVersion() {
		String minor = System.getenv("TARANTOOL_VERSION");
		minor = minor == null ? "8" : minor;
		String majorMinor = "1." + minor;
		return majorMinor;
	}

	@Test
	public void testExecuteCreateTable() {
		Assume.assumeTrue(getEnvTarantoolVersion().startsWith("1.8"));
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.execute("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))");
			assertEquals(1L, client.executeUpdate());
			client.execute("DROP TABLE table1");
			assertEquals(1L, client.executeUpdate());
		}
	}

	@Test
	public void testInsertAndSelect() {
		Assume.assumeTrue(getEnvTarantoolVersion().startsWith("1.8"));
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.execute("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))");
			assertEquals(1L, client.executeUpdate());

			client.execute("INSERT INTO table1 values(?,?)");
			client.setInt(1);
			client.setString("A");
			assertEquals(1L, client.executeUpdate());

			// TODO select

			client.execute("DROP TABLE table1");
			assertEquals(1L, client.executeUpdate());
		}
	}

}
