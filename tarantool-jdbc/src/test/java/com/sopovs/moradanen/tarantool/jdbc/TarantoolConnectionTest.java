package com.sopovs.moradanen.tarantool.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.sopovs.moradanen.tarantool.MapResult;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientImpl;

public class TarantoolConnectionTest {

	@Before
	public void setup() {
		assumeTrue(getEnvTarantoolVersion().startsWith("1.8"));
	}

	private static String getEnvTarantoolVersion() {
		String minor = System.getenv("TARANTOOL_VERSION");
		minor = minor == null ? "8" : minor;
		String majorMinor = "1." + minor;
		return majorMinor;
	}

	private void testOneSelect(String value, Consumer<TarantoolResultSet> resConsumer) throws SQLException {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				TarantoolConnection con = new TarantoolConnection(client);
				TarantoolStatement st = con.createStatement()) {
			st.executeUpdate("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))");

			try (PreparedStatement pst = con.prepareStatement("INSERT INTO table1 values(?,?)")) {
				pst.setString(2, value);
				pst.setInt(1, 1);
				assertEquals(1, pst.executeUpdate());
			}

			TarantoolResultSet res = st.executeQuery("select * from table1");
			assertTrue(res.next());
			assertTrue(res.isWrapperFor(MapResult.class));
			assertEquals(1, res.getInt(1));
			assertEquals(1, res.getInt("column1"));
			assertFalse(res.wasNull());

			resConsumer.accept(res);

			assertFalse(res.next());
			st.executeUpdate("DROP TABLE table1");
		}
	}

	@Test
	public void testSimple() throws SQLException {

		testOneSelect("Foobar", res -> {
			try {
				assertEquals("Foobar", res.getString(2));
				assertEquals("Foobar", res.getString("column2"));
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testWasNullGetByte() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertEquals(0, res.getByte("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testWasNullGetShort() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertEquals(0, res.getShort("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testWasNullGetInt() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertEquals(0, res.getInt("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testWasNullGetLong() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertEquals(0, res.getLong("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testWasNullGetString() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertNull(res.getString("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testWasNullGetObject() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertNull(res.getObject("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}
	
	@Test
	public void testWasNullGetRef() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertNull(res.getRef("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}
	
	@Test
	public void testWasNullGetBlob() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertNull(res.getBlob("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}
	
	@Test
	public void testWasNullGetClob() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertNull(res.getClob("column2"));
				assertTrue(res.wasNull());
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Test
	public void testFindColumn() throws SQLException {
		testOneSelect(null, res -> {
			try {
				assertEquals(1, res.findColumn("column1"));
				assertEquals(2, res.findColumn("column2"));
			} catch (SQLException e) {
				throw new AssertionError(e);
			}

		});
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testMissingFirstParameterExecute() throws SQLException {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				TarantoolConnection con = new TarantoolConnection(client);
				TarantoolPreparedStatement pst = con.prepareStatement("foobar")) {
			pst.setString(2, "foobar");
			thrown.expect(SQLException.class);
			thrown.expectMessage("Parameter 1 is not set");
			pst.executeQuery();
		}
	}

	@Test
	public void testMissingFirstParameterUpdate() throws SQLException {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				TarantoolConnection con = new TarantoolConnection(client);
				TarantoolPreparedStatement pst = con.prepareStatement("foobar")) {
			pst.setString(2, "foobar");
			thrown.expect(SQLException.class);
			thrown.expectMessage("Parameter 1 is not set");
			pst.executeUpdate();
		}
	}
}
