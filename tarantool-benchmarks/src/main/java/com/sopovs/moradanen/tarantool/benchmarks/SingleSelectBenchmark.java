package com.sopovs.moradanen.tarantool.benchmarks;

import com.sopovs.moradanen.tarantool.*;
import com.sopovs.moradanen.tarantool.core.Iter;
import org.openjdk.jmh.annotations.*;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolConnection;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

//Benchmark                                                          size)         Score          Error   Units
//SingleSelectBenchmark.client                                           1        57.178 ±        5.543   us/op
//SingleSelectBenchmark.client:·gc.alloc.rate.norm                       1      1031.907 ±        3.963    B/op
//SingleSelectBenchmark.client                                         100        90.037 ±        9.848   us/op
//SingleSelectBenchmark.client:·gc.alloc.rate.norm                     100     54548.157 ±       42.209    B/op
//SingleSelectBenchmark.client                                       10000      3140.693 ±     1011.209   us/op
//SingleSelectBenchmark.client:·gc.alloc.rate.norm                   10000   5640838.005 ±       32.307    B/op
//
//SingleSelectBenchmark.jdbc                                             1        71.664 ±       13.217   us/op
//SingleSelectBenchmark.jdbc:·gc.alloc.rate.norm                         1      1622.329 ±        6.379    B/op
//SingleSelectBenchmark.jdbc                                           100       159.138 ±      211.577   us/op
//SingleSelectBenchmark.jdbc:·gc.alloc.rate.norm                       100     54372.200 ±        1.983    B/op
//SingleSelectBenchmark.jdbc                                         10000      6888.826 ±     3551.356   us/op
//SingleSelectBenchmark.jdbc:·gc.alloc.rate.norm                     10000   5770314.266 ±       41.093    B/op
//
//SingleSelectBenchmark.upstreamConnection                               1        79.236 ±       30.518   us/op
//SingleSelectBenchmark.upstreamConnection:·gc.alloc.rate.norm           1     12224.092 ±        0.480    B/op
//SingleSelectBenchmark.upstreamConnection                             100       390.002 ±       75.460   us/op
//SingleSelectBenchmark.upstreamConnection:·gc.alloc.rate.norm         100    129064.461 ±        2.542    B/op
//SingleSelectBenchmark.upstreamConnection                           10000     36831.683 ±     3929.321   us/op
//SingleSelectBenchmark.upstreamConnection:·gc.alloc.rate.norm       10000  12598251.881 ±      281.982    B/op
//
//SingleSelectBenchmark.upstreamClient                                   1        73.421 ±        9.820   us/op
//SingleSelectBenchmark.upstreamClient:·gc.alloc.rate.norm               1     10640.085 ±     8816.478    B/op
//SingleSelectBenchmark.upstreamClient                                 100       117.286 ±       98.430   us/op
//SingleSelectBenchmark.upstreamClient:·gc.alloc.rate.norm             100     98409.737 ±   197746.872    B/op
//SingleSelectBenchmark.upstreamClient                               10000      3411.124 ±       68.027   us/op
//SingleSelectBenchmark.upstreamClient:·gc.alloc.rate.norm           10000   9126879.306 ± 19632126.260    B/op
//
//SingleSelectBenchmark.upstreamJdbc                                     1        96.783 ±        1.420   us/op
//SingleSelectBenchmark.upstreamJdbc:·gc.alloc.rate.norm                 1     16872.114 ±        0.632    B/op
//SingleSelectBenchmark.upstreamJdbc                                   100       462.498 ±       91.592   us/op
//SingleSelectBenchmark.upstreamJdbc:·gc.alloc.rate.norm               100    137456.603 ±        2.907    B/op
//SingleSelectBenchmark.upstreamJdbc                                 10000     40162.712 ±      896.595   us/op
//SingleSelectBenchmark.upstreamJdbc:·gc.alloc.rate.norm             10000  13011867.503 ±      318.996    B/op

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class SingleSelectBenchmark {

    private TarantoolConnection connection;
    private org.tarantool.TarantoolClient referenceClient;
    private TarantoolClientSource clientSource;
    private TarantoolTemplate template;
    private Connection jdbcConnection;
    private Connection upstreamJdbcConnection;
    private int space;

    @Param({"1", "100", "10000"})
    public int size;

    @Setup
    public void setup() throws Exception {
        SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
        TarantoolClientConfig tarantoolClientConfig = new TarantoolClientConfig();
        tarantoolClientConfig.username = "admin";
        tarantoolClientConfig.password = "javapass";
        referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel, tarantoolClientConfig);
        connection = new TarantoolConnection("admin", "javapass", new Socket("localhost", 3301));
        clientSource = new TarantoolPooledClientSource("localhost", 3301, "admin", "javapass", 1);

        template = new TarantoolTemplate(clientSource);
        jdbcConnection = new com.sopovs.moradanen.tarantool.jdbc.TarantoolConnection(new TarantoolClientImpl("localhost", 3301, "admin", "javapass"));
        upstreamJdbcConnection = new org.tarantool.jdbc.SQLDriver().connect("tarantool://localhost:3301?user=admin&password=javapass", new Properties());
        setupData();

    }

    private void setupData() throws SQLException {
        try (Statement st = jdbcConnection.createStatement();
             PreparedStatement pst = jdbcConnection.prepareStatement("INSERT INTO JDBCBENCHMARK VALUES(?,?)")) {
            st.executeUpdate("CREATE TABLE JDBCBENCHMARK(C1 INTEGER PRIMARY KEY, C2 VARCHAR(100))");
            for (int i = 0; i < size; i++) {
                pst.setInt(1, i);
                pst.setString(2, "FooBar" + i);
                pst.addBatch();
            }
            pst.executeBatch();
        }
        try (TarantoolClient client = clientSource.getClient()) {
            space = client.space("JDBCBENCHMARK");
        }
    }

    @Benchmark
    public List<Foo> jdbc() throws SQLException {
        return jdbcInternal(jdbcConnection);
    }

    @Benchmark
    public List<Foo> upstreamJdbc() throws SQLException {
       return jdbcInternal(upstreamJdbcConnection);
    }
    public List<Foo> jdbcInternal(Connection con) throws SQLException {
        List<Foo> result = new ArrayList<>();
        try (PreparedStatement pst = con.prepareStatement("SELECT * FROM JDBCBENCHMARK");
             ResultSet res = pst.executeQuery()) {
            while (res.next()) {
                result.add(new Foo(res.getInt(1), res.getString(2)));
            }
        }
        return result;
    }


    @Benchmark
    public List<Foo> client() {
        return template.selectAndMapAll(space, res -> new Foo(res.getInt(0), res.getString(1)));
    }

    public static final class Foo {
        private final int id;
        private final String val;

        public Foo(int id, String val) {
            this.id = id;
            this.val = val;
        }

        public int getId() {
            return id;
        }

        public String getVal() {
            return val;
        }
    }

    @Benchmark
    public List<?> upstreamConnection() {
        return connection.select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE, Iter.ALL.getValue());
    }

    @Benchmark
    public List<?> upstreamClient() {
        return referenceClient.syncOps().select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE,
                Iter.ALL.getValue());
    }

    @TearDown
    public void tearDown() throws SQLException {
        try (Statement st = jdbcConnection.createStatement()) {
            st.executeUpdate("DROP TABLE JDBCBENCHMARK");
        }

        connection.close();
        referenceClient.close();
        clientSource.close();
        jdbcConnection.close();
    }
}
