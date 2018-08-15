package com.sopovs.moradanen.tarantool.jdbc;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.sopovs.moradanen.tarantool.test.TestUtil.getEnvTarantoolVersion;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class DriverManagerTest {
    @BeforeAll
    static void setup() {
        assumeTrue(getEnvTarantoolVersion().startsWith("2.0"));
    }

    @Test
    void test() throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:tarantool://localhost")) {
            assertTrue(con instanceof TarantoolConnection);
            assertTrue(con.isValid(0));
        }
    }
}
