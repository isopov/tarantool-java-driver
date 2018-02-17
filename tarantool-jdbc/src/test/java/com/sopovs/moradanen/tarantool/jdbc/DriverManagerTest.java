package com.sopovs.moradanen.tarantool.jdbc;

import static com.sopovs.moradanen.tarantool.jdbc.TarantoolConnectionTest.getEnvTarantoolVersion;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

public class DriverManagerTest {
	@Before
	public void setup() {
		assumeTrue(getEnvTarantoolVersion().startsWith("2.0"));
	}
	
	@Test
	public void test() throws SQLException {
		try(Connection con = DriverManager.getConnection("jdbc:tarantool://localhost")){
			assertTrue(con instanceof TarantoolConnection);
			assertTrue(con.isValid(0));
		}
	}
}
