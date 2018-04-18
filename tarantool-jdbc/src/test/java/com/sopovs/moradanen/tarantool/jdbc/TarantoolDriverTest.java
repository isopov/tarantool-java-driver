package com.sopovs.moradanen.tarantool.jdbc;

import org.junit.Test;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class TarantoolDriverTest {

    @Test
    public void testUrlParsing() throws SQLException {
        TarantoolDriver driver = new TarantoolDriver();
        DriverPropertyInfo[] props = driver.getPropertyInfo("jdbc:tarantool://localhost:3301?user=foo&password=bar", null);
        assertEquals("host", props[0].name);
        assertEquals("localhost", props[0].value);

        assertEquals("port", props[1].name);
        assertEquals("3301", props[1].value);

        assertEquals("user", props[2].name);
        assertEquals("foo", props[2].value);

        assertEquals("password", props[3].name);
        assertEquals("bar", props[3].value);
    }

}
