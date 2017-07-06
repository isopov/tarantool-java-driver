package com.sopovs.moradanen.tarantool;

import static com.sopovs.moradanen.tarantool.TarantoolClientImplTest.testAuth;

import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TarantoolClientImplExceptionsTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

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
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
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

	private void testSetWithoutAction(Consumer<TarantoolClient> setter) {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage("Need to call one of update/insert/upsert/delete from setting tuple value");
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			setter.accept(client);
		}
	}

	@Test
	public void testExecuteWithoutAction() {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage("Trying to execute absent query");
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			client.execute();
		}
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

	private void testPreActionCheck(Consumer<TarantoolClient> action) {
		thrown.expect(TarantoolException.class);
		thrown.expectMessage("Execute or add to batch action before starting next one");
		try (TarantoolClientImpl client = new TarantoolClientImpl("localhost")) {
			action.accept(client);
			action.accept(client);
		}
	}
}
