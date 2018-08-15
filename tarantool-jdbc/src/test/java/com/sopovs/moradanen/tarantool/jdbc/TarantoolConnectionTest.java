package com.sopovs.moradanen.tarantool.jdbc;

import com.sopovs.moradanen.tarantool.SqlResult;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import static com.sopovs.moradanen.tarantool.test.TestUtil.getEnvTarantoolVersion;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TarantoolConnectionTest {

    @BeforeAll
    static void setup() {
        assumeTrue(getEnvTarantoolVersion().startsWith("2.0"));
    }


    private void testOneSelect(String value, Consumer<TarantoolResultSet> resConsumer) throws SQLException {
        try (TarantoolClient client = new TarantoolClientImpl("localhost");
             TarantoolConnection con = new TarantoolConnection(client);
             TarantoolStatement st = con.createStatement()) {
            st.executeUpdate("CREATE TABLE TABLE1 (COLUMN1 INTEGER PRIMARY KEY, COLUMN2 VARCHAR(100))");

            try (PreparedStatement pst = con.prepareStatement("INSERT INTO TABLE1 VALUES(?,?)")) {
                pst.setString(2, value);
                pst.setInt(1, 1);
                assertEquals(1, pst.executeUpdate());
            }

            TarantoolResultSet res = st.executeQuery("SELECT * FROM table1");
            assertTrue(res.next());
            assertTrue(res.isWrapperFor(SqlResult.class));
            assertEquals(1, res.getInt(1));
            assertEquals(1, res.getInt("COLUMN1"));
            assertFalse(res.wasNull());

            resConsumer.accept(res);

            assertFalse(res.next());
            st.executeUpdate("DROP TABLE TABLE1");
        }
    }

    @Test
    void testSimple() throws SQLException {

        testOneSelect("Foobar", res -> {
            try {
                assertEquals("Foobar", res.getString(2));
                assertEquals("Foobar", res.getString("COLUMN2"));
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetByte() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertEquals(0, res.getByte("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetShort() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertEquals(0, res.getShort("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetInt() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertEquals(0, res.getInt("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetLong() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertEquals(0, res.getLong("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetString() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertNull(res.getString("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetObject() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertNull(res.getObject("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetRef() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertNull(res.getRef("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetBlob() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertNull(res.getBlob("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testWasNullGetClob() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertNull(res.getClob("COLUMN2"));
                assertTrue(res.wasNull());
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }

    @Test
    void testFindColumn() throws SQLException {
        testOneSelect(null, res -> {
            try {
                assertEquals(1, res.findColumn("COLUMN1"));
                assertEquals(2, res.findColumn("COLUMN2"));
            } catch (SQLException e) {
                throw new AssertionError(e);
            }

        });
    }


    @Test
    void testBatch() throws SQLException {
        try (TarantoolClient client = new TarantoolClientImpl("localhost");
             TarantoolConnection con = new TarantoolConnection(client);
             TarantoolStatement st = con.createStatement()) {
            st.executeUpdate("CREATE TABLE TABLE1 (COLUMN1 INTEGER PRIMARY KEY, COLUMN2 VARCHAR(100))");

            try (TarantoolPreparedStatement pst = con.prepareStatement("INSERT INTO TABLE1 VALUES(?,?)")) {
                for (int i = 0; i < 10; i++) {
                    pst.setInt(1, i);
                    pst.setString(2, "FooBar" + i);
                    pst.addBatch();
                }
                pst.executeBatch();
            }

            TarantoolResultSet res = st.executeQuery("SELECT COUNT(*) FROM TABLE1");
            assertTrue(res.next());
            assertEquals(10, res.getInt(1));

            assertFalse(res.next());
            st.executeUpdate("DROP TABLE TABLE1");
        }
    }

    @Test
    void testMissingFirstParameterExecute() throws SQLException {
        try (TarantoolClient client = new TarantoolClientImpl("localhost");
             TarantoolConnection con = new TarantoolConnection(client);
             TarantoolPreparedStatement pst = con.prepareStatement("foobar")) {
            pst.setString(2, "foobar");

            assertThrows(SQLException.class,
                    pst::executeQuery,
                    "Parameter 1 is not set"
            );
        }
    }

    @Test
    void testMissingFirstParameterUpdate() throws SQLException {
        try (TarantoolClient client = new TarantoolClientImpl("localhost");
             TarantoolConnection con = new TarantoolConnection(client);
             TarantoolPreparedStatement pst = con.prepareStatement("foobar")) {
            pst.setString(2, "foobar");


            assertThrows(SQLException.class,
                    pst::executeUpdate,
                    "Parameter 1 is not set"
            );
        }
    }
}
