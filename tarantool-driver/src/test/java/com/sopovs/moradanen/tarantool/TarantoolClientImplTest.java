package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.Op;
import com.sopovs.moradanen.tarantool.core.Util;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static com.sopovs.moradanen.tarantool.test.TestUtil.getEnvTarantoolVersion;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


class TarantoolClientImplTest {
    @Test
    void testConnect() {
        //noinspection EmptyTryBlock
        try (TarantoolClient ignored = new TarantoolClientImpl("localhost")) {
        }
    }

    @Test
    void testPing() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
            client.ping();
        }
    }

    @Test
    void testSelect() {
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
    void testSelectByName() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            client.selectAll("_vspace", 3);
            Result result = client.execute();
            assertEquals(3, result.getSize());
            result.consume();
            assertFalse(result.hasNext());
        }
    }

    @Test
    void testManySelects() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
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
    void testSpace() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
//            assertEquals(Util.SPACE_SCHEMA, client.space("_schema"));
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
    void testEval() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            client.evalFully("box.schema.space.create('javatest')");
            client.evalFully("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'num'}})");
            client.evalFully("box.space.javatest:drop()");
        }
    }

    @Test
    void testInsert() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            insertInternal(client);
        }
    }

    @Test
    void testReplace() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            createTestSpace(client);

            client.replace("javatest");
            client.setInt(1);
            client.setInt(0);
            client.setString("Foobar");

            insertCheck(client);
        }
    }

    @Test
    void testDelete() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
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

    @Test
    void testDeleteComposite() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {

            client.evalFully("box.schema.space.create('javatest')");
            client.evalFully(
                    "box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'num', 2, 'num'}})");

            client.insert("javatest");
            client.setInt(1);
            client.setInt(0);
            client.execute().consume();

            client.insert("javatest");
            client.setInt(1);
            client.setInt(1);
            client.execute().consume();

            client.delete("javatest");
            client.setInt(1);
            client.setInt(1);
            Result delete = client.execute();
            assertEquals(1, delete.getSize());
            delete.consume();

            client.selectAll("javatest");
            Result select = client.execute();
            assertEquals(1, select.getSize());
            select.next();
            assertEquals(1, select.getInt(0));
            assertEquals(0, select.getInt(1));
        }
    }

    private static void insertInternal(TarantoolClient client) {
        createTestSpace(client);

        client.insert("javatest");
        client.setInt(1);
        client.setInt(0);
        client.setString("Foobar");

        insertCheck(client);
    }

    private static void insertCheck(TarantoolClient client) {
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

    static void createTestSpace(TarantoolClient client) {
        client.evalFully("box.schema.space.create('javatest')");
        client.evalFully("box.space.javatest:create_index('primary', {type = 'hash', parts = {1, 'num'}})");
    }

    @Test
    void testInsertBatch() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
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
    void testUpdate() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            insertInternal(client);
            client.update(client.space("javatest"), 0);
            client.setInt(1);
            client.change(IntOp.PLUS, 1, 1);
            Result update = client.execute();
            assertEquals(1, update.getSize());

            update.consume();

            testValue(client, 1);
        }
    }

    @Test
    void testUpdateLong() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            insertInternal(client);
            client.update(client.space("javatest"), 0);
            client.setInt(1);
            long val = Integer.MAX_VALUE;
            val *= 3;
            client.change(IntOp.PLUS, 1, val);
            Result update = client.execute();
            assertEquals(1, update.getSize());
            update.consume();
            testValue(client, val);
        }
    }

    @Test
    void testUpdateIntAssign() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            insertInternal(client);
            client.update(client.space("javatest"), 0);
            client.setInt(1);
            client.change(IntOp.ASSIGN, 1, 42);
            Result update = client.execute();
            assertEquals(1, update.getSize());

            update.consume();

            testValue(client, 42);
        }
    }

    @Test
    void testUpdateBytes() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {

            createTestSpace(client);

            client.insert("javatest");
            client.setInt(1);
            client.setBytes(new byte[]{0, 0, 0, 0});
            Result insert = client.execute();
            assertEquals(1, insert.getSize());
            insert.consume();

            client.update(client.space("javatest"), 0);
            client.setInt(1);
            byte[] newBytes = new byte[]{1, 1, 1, 1, 1};
            client.change(Op.ASSIGN, 1, newBytes);
            Result update = client.execute();
            assertEquals(1, update.getSize());

            update.consume();

            client.select("javatest", 0);
            client.setInt(1);
            Result select = client.execute();
            assertEquals(1, select.getSize());
            select.next();
            assertEquals(1, select.getInt(0));
            assertArrayEquals(newBytes, select.getBytes(1));
        }
    }

    @Test
    void testUpdateString() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            insertInternal(client);
            client.update(client.space("javatest"), 0);
            client.setInt(1);
            client.change(Op.ASSIGN, 2, "Barfoo");
            Result update = client.execute();
            assertEquals(1, update.getSize());

            update.consume();

            testValue(client, 0, "Barfoo");
        }
    }

    @Test
    void testUpdateIntString() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            insertInternal(client);
            client.update(client.space("javatest"), 0);
            client.setInt(1);
            client.change(IntOp.PLUS, 1, 1);
            client.change(Op.ASSIGN, 2, "Barfoo");
            Result update = client.execute();
            assertEquals(1, update.getSize());
            update.consume();
            testValue(client, 1, "Barfoo");
        }
    }

    @Test
    void testUpsert() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            createTestSpace(client);

            testUpsertStep(client, 1);
            testUpsertStep(client, 2);
            testUpsertStep(client, 3);
        }
    }

    @Test
    void testUpsertBatch() throws Exception {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {
            createTestSpace(client);
            for (int i = 0; i < 10; i++) {
                testUpsertBatchStep(client);
                client.addBatch();
            }
            client.executeBatch();
            testValue(client, 10);
        }
    }

    private void testUpsertStep(TarantoolClient client, int value) {
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
        client.change(IntOp.PLUS, 1, 1);
    }

    private void testValue(TarantoolClient client, long value) {
        testValue(client, value, "Foobar");
    }

    private void testValue(TarantoolClient client, long intValue, String strValue) {
        client.select("javatest", 0);
        client.setInt(1);
        Result select = client.execute();
        assertEquals(1, select.getSize());
        select.next();
        assertEquals(1, select.getInt(0));
        assertEquals(intValue, select.getLong(1));
        assertEquals(strValue, select.getString(2));
    }

    @Test
    void testGetVersion() {
        String majorMinor = getEnvTarantoolVersion();
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            assertTrue(client.getVersion().startsWith(majorMinor));
        }
    }

    @Test
    void testExecuteCreateTable() {
        sqlTest(ignored -> {
        });
    }

    private void sqlTest(Consumer<TarantoolClient> work) {
        assumeTrue(getEnvTarantoolVersion().startsWith("2.1"));
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            client.sql("CREATE TABLE TABLE1 (COLUMN1 INTEGER PRIMARY KEY, COLUMN2 VARCHAR(100))");
            assertEquals(1L, client.executeUpdate());

            work.accept(client);

            client.sql("DROP TABLE TABLE1");
            assertEquals(1L, client.executeUpdate());
        }
    }

    @Test
    void testInsertAndSelect() {
        sqlTest(client -> {
            client.sql("INSERT INTO TABLE1 VALUES(?,?)");
            client.setInt(1);
            client.setString("A");
            assertEquals(1L, client.executeUpdate());

            client.sql("select * from TABLE1");
            Result result = client.execute();
            assertTrue(result instanceof SqlResult);
            SqlResult mapResult = (SqlResult) result;
            assertEquals(2, mapResult.getFieldNames().size());
            assertEquals(0, mapResult.getIndex("COLUMN1"));
            assertEquals(1, mapResult.getIndex("COLUMN2"));
            assertTrue(mapResult.hasNext());
            assertTrue(mapResult.next());

            assertEquals(1, mapResult.getInt(mapResult.getIndex("COLUMN1")));
            assertEquals("A", mapResult.getString(mapResult.getIndex("COLUMN2")));
            assertFalse(mapResult.hasNext());
            assertFalse(mapResult.next());
        });
    }

    @Test
    void testIsClosed() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            assertFalse(client.isClosed());
            client.ping();
            client.close();
            assertTrue(client.isClosed());
        }
    }
}
