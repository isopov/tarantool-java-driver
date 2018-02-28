package com.sopovs.moradanen.tarantool;

import static com.sopovs.moradanen.tarantool.TarantoolClientImpl.EXECUTE_ABSENT_EXCEPTION;
import static com.sopovs.moradanen.tarantool.TarantoolClientImpl.PRE_ACTION_EXCEPTION;
import static com.sopovs.moradanen.tarantool.TarantoolClientImpl.PRE_CHANGE_EXCEPTION;
import static com.sopovs.moradanen.tarantool.TarantoolClientImpl.PRE_SET_EXCEPTION;
import static com.sopovs.moradanen.tarantool.TarantoolClientImplTest.testAuth;

import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.TarantoolAuthException;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

public class TarantoolClientImplExceptionsTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testClosedPing() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.close();
			thrown.expect(TarantoolException.class);
			client.ping();
		}
	}

	@Test
	public void testWrongPassword() {
		thrown.expect(TarantoolAuthException.class);
		thrown.expectMessage("Incorrect password supplied for user 'foobar'");
		testAuth("foobar", "barfoo");
	}

	@Test
	public void testWrongUser() {
		thrown.expect(TarantoolAuthException.class);
		thrown.expectMessage("User 'barfoo' is not found");
		testAuth("barfoo", "barfoo");
	}

	@Test
	public void testNoSpace() {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage("No such space no_such_space");
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.space("no_such_space");
		}
	}

	@Test
	public void testSetIntWithoutInsert() {
		testSetWithoutAction(c -> c.setInt(1));
	}

	@Test
	public void testSetLongWithoutInsert() {
		testSetWithoutAction(c -> c.setLong(1L));
	}

	@Test
	public void testSetFloatWithoutInsert() {
		testSetWithoutAction(c -> c.setFloat(0F));
	}

	@Test
	public void testSetDoubleWithoutInsert() {
		testSetWithoutAction(c -> c.setDouble(0D));
	}

	@Test
	public void testSetBooleanWithoutInsert() {
		testSetWithoutAction(c -> c.setBoolean(false));
	}

	@Test
	public void testSetNullWithoutInsert() {
		testSetWithoutAction(TarantoolClient::setNull);
	}

	@Test
	public void testChangeWithoutUpdate() {
		testException(PRE_CHANGE_EXCEPTION, c -> c.change(IntOp.AND, 1, 1));
	}

	@Test
	public void testChangeAfterInsert() {
		testException(PRE_CHANGE_EXCEPTION, c -> {
			c.insert(42);
			c.change(IntOp.AND, 1, 1);
		});
	}

	@Test
	public void testChangeAfterSelect() {
		testException(PRE_CHANGE_EXCEPTION, c -> {
			c.select(42, 1);
			c.change(IntOp.AND, 1, 1);
		});
	}

	@Test
	public void testExecuteWithoutAction() {
		testException(EXECUTE_ABSENT_EXCEPTION, TarantoolClient::execute);
	}

	@Test
	public void testDoubleEvalWithoutExecute() {
		testPreActionCheck(c -> c.eval("foobar"));
	}

	@Test
	public void testDoubleSelectWithoutExecute() {
		testPreActionCheck(c -> c.select(42, 1));
	}

	@Test
	public void testDoubleInsertWithoutExecute() {
		testPreActionCheck(c -> c.insert(42));
	}

	@Test
	public void testDoubleUpdateWithoutExecute() {
		testPreActionCheck(c -> c.update(42, 1));
	}

	@Test
	public void testDoubleUpsertWithoutExecute() {
		testPreActionCheck(c -> c.upsert(42));
	}

	private void testException(String message, Consumer<TarantoolClient> setter) {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage(message);
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			setter.accept(client);
		}
	}

	private void testExceptionDoubleAccept(String message, Consumer<TarantoolClient> action) {
		testException(message, c -> {
			action.accept(c);
			action.accept(c);
		});
	}

	private void testSetWithoutAction(Consumer<TarantoolClient> setter) {
		testException(PRE_SET_EXCEPTION, setter);
	}

	private void testPreActionCheck(Consumer<TarantoolClient> action) {
		testExceptionDoubleAccept(PRE_ACTION_EXCEPTION, action);
	}

}
