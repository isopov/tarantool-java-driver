package com.sopovs.moradanen.tarantool.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

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

	@Test
	public void testSimple() throws SQLException {
		try (TarantoolClient client = new TarantoolClientImpl("localhost");
				TarantoolConnection con = new TarantoolConnection(client);
				Statement st = con.createStatement()) {
			st.executeUpdate("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))");

			// TODO PreparedStatement
			client.execute("INSERT INTO table1 values(?,?)");
			client.setInt(1);
			client.setString("Foobar");
			assertEquals(1L, client.executeUpdate());

			ResultSet res = st.executeQuery("select * from table1");
			assertTrue(res.isWrapperFor(MapResult.class));
			assertTrue(res.next());
			assertEquals(1, res.getInt(1));
			assertEquals(1, res.getInt("column1"));

			assertEquals("Foobar", res.getString(2));
			assertEquals("Foobar", res.getString("column2"));

			assertFalse(res.next());

			st.executeUpdate("DROP TABLE table1");
		}
	}

}
