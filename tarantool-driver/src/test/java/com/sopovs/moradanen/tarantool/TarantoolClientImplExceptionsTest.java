package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.TarantoolException;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static com.sopovs.moradanen.tarantool.TarantoolClientImpl.*;
import static com.sopovs.moradanen.tarantool.TarantoolClientImplTest.createTestSpace;
import static com.sopovs.moradanen.tarantool.TarantoolClientImplTest.testAuth;
import static org.junit.jupiter.api.Assertions.*;


//TODO - some sort of parameterized tests (maybe with junit5)
class TarantoolClientImplExceptionsTest {

    @Test
    void testClosedPing() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
            client.close();
            assertThrows(TarantoolException.class, client::ping);
        }
    }

    @Test
    void testWrongPassword() {
        TarantoolException exception = assertThrows(TarantoolException.class,
                () -> testAuth("foobar", "barfoo")
        );
        assertEquals("Incorrect password supplied for user 'foobar'", exception.getMessage());
    }

    @Test
    void testWrongUser() {
        assertThrows(TarantoolException.class,
                () -> testAuth("barfoo", "barfoo"),
                "User 'barfoo' is not found"
        );
    }

    @Test
    void testNoSpace() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
            assertThrows(TarantoolException.class,
                    () -> client.space("no_such_space"),
                    "No such space no_such_space"
            );
        }
    }

    @Test
    void testSetIntWithoutInsert() {
        testSetWithoutAction(c -> c.setInt(1));
    }

    @Test
    void testSetLongWithoutInsert() {
        testSetWithoutAction(c -> c.setLong(1L));
    }

    @Test
    void testSetFloatWithoutInsert() {
        testSetWithoutAction(c -> c.setFloat(0F));
    }

    @Test
    void testSetDoubleWithoutInsert() {
        testSetWithoutAction(c -> c.setDouble(0D));
    }

    @Test
    void testSetBooleanWithoutInsert() {
        testSetWithoutAction(c -> c.setBoolean(false));
    }

    @Test
    void testSetNullWithoutInsert() {
        testSetWithoutAction(TarantoolClient::setNull);
    }

    @Test
    void testChangeWithoutUpdate() {
        testException(PRE_CHANGE_EXCEPTION, c -> c.change(IntOp.AND, 1, 1));
    }

    @Test
    void testChangeAfterInsert() {
        testException(PRE_CHANGE_EXCEPTION, c -> {
            c.insert(42);
            c.change(IntOp.AND, 1, 1);
        });
    }

    @Test
    void testChangeAfterSelect() {
        testException(PRE_CHANGE_EXCEPTION, c -> {
            c.select(42, 1);
            c.change(IntOp.AND, 1, 1);
        });
    }

    @Test
    void testExecuteWithoutAction() {
        testException(EXECUTE_ABSENT_EXCEPTION, TarantoolClient::execute);
    }

    @Test
    void testDoubleEvalWithoutExecute() {
        testPreActionCheck(c -> c.eval("foobar"));
    }

    @Test
    void testDoubleSelectWithoutExecute() {
        testPreActionCheck(c -> c.select(42, 1));
    }

    @Test
    void testDoubleInsertWithoutExecute() {
        testPreActionCheck(c -> c.insert(42));
    }

    @Test
    void testDoubleUpdateWithoutExecute() {
        testPreActionCheck(c -> c.update(42, 1));
    }

    @Test
    void testDoubleUpsertWithoutExecute() {
        testPreActionCheck(c -> c.upsert(42));
    }

    @Test
    void testGetIntNull() {
        testSelectNull(result -> result.getInt(1), "Expected integer, but got null");
    }

    @Test
    void testGetIntString() {
        testSelectString(result -> result.getInt(1), "Expected integer, but got string");
    }

    @Test
    void testGetFloatNull() {
        testSelectNull(result -> result.getFloat(1), "Expected float, but got null");
    }

    @Test
    void testGetFloatString() {
        testSelectString(result -> result.getFloat(1), "Expected float, but got string");
    }

    @Test
    void testGetLongNull() {
        testSelectNull(result -> result.getLong(1), "Expected integer, but got null");
    }

    @Test
    void testGetLongString() {
        testSelectString(result -> result.getLong(1), "Expected integer, but got string");
    }

    @Test
    void testGetDoubleNull() {
        testSelectNull(result -> result.getDouble(1), "Expected float, but got null");
    }

    @Test
    void testGetDoubleString() {
        testSelectString(result -> result.getDouble(1), "Expected float, but got string");
    }

    @Test
    void testGetBooleanNull() {
        testSelectNull(result -> result.getBoolean(1), "Expected boolean, but got null");
    }

    @Test
    void testGetBooleanString() {
        testSelectString(result -> result.getBoolean(1), "Expected boolean, but got string");
    }

    @Test
    void testGetStringNull() {
        testSelectNull(result -> assertNull(result.getString(1)));
    }

    @Test
    void testGetStringDouble() {
        testSelectDouble(result -> result.getString(1), "Expected string, but got float");
    }


    @Test
    void testGetBooleanDouble() {
        testSelectDouble(result -> result.getBoolean(1), "Expected boolean, but got float");
    }

    @Test
    void testGetBytesNull() {
        testSelectNull(result -> result.getBytes(1), "Expected binary, but got null");
    }

    @Test
    void testGetBytesString() {
        testSelectString(result -> result.getBytes(1), "Expected binary, but got string");
    }

    @Test
    void testGetByteBufferNull() {
        testSelectNull(result -> result.getByteBuffer(1), "Expected binary, but got null");
    }

    @Test
    void testGetByteBufferString() {
        testSelectString(result -> result.getByteBuffer(1), "Expected binary, but got string");
    }

    private static void testSelectString(Consumer<Result> getter, String getterException) {
        testSetAndSelect(client -> client.setString("foobar"), getter, getterException);
    }

    private static void testSelectNull(Consumer<Result> getter) {
        testSetAndSelect(TarantoolClient::setNull, getter);
    }

    private static void testSelectNull(Consumer<Result> getter, String getterException) {
        testSetAndSelect(TarantoolClient::setNull, getter, getterException);
    }

    private static void testSelectDouble(Consumer<Result> getter, String getterException) {
        testSetAndSelect(client -> client.setDouble(42D), getter, getterException);
    }

    private static void testSetAndSelect(Consumer<TarantoolClient> setter, Consumer<Result> getter) {
        testSetAndSelect(setter, getter, null);
    }

    private static void testSetAndSelect(Consumer<TarantoolClient> setter, Consumer<Result> getter, String getterException) {
        try (TarantoolClient client = new TarantoolClientImpl("localhost");
             AutoCloseable ignored = () -> client.evalFully("box.space.javatest:drop()")) {

            createTestSpace(client);

            client.insert("javatest");
            client.setInt(1);
            setter.accept(client);

            Result insert = client.execute();
            assertEquals(1, insert.getSize());
            insert.consume();

            client.select(client.space("javatest"), 0);
            client.setInt(1);
            Result first = client.execute();
            assertEquals(1, first.getSize());
            first.next();
            if (getterException != null) {
                TarantoolException exception = assertThrows(TarantoolException.class, () -> getter.accept(first));
                assertEquals(getterException, exception.getMessage());
            } else {
                getter.accept(first);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void testException(String message, Consumer<TarantoolClient> setter) {
        try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
            assertThrows(TarantoolException.class,
                    () -> setter.accept(client),
                    message
            );
        }
    }

    private void testSetWithoutAction(Consumer<TarantoolClient> setter) {
        testException(PRE_SET_EXCEPTION, setter);
    }

    private void testPreActionCheck(Consumer<TarantoolClient> action) {
        testException(PRE_ACTION_EXCEPTION, c -> {
            action.accept(c);
            action.accept(c);
        });
    }
}
