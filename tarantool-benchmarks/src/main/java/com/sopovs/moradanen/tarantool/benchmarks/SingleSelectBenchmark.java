package com.sopovs.moradanen.tarantool.benchmarks;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolConnection;

import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientImpl;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.TarantoolPooledClientSource;
import com.sopovs.moradanen.tarantool.TarantoolTemplate;
import com.sopovs.moradanen.tarantool.core.Iter;

//Benchmark                                            (size)        Score          Error   Units
//SelectBenchmark.client                                    1       60.051 ±       19.458   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                1      747.871 ±       64.017    B/op
//SelectBenchmark.client                                  100      109.769 ±       32.617   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm              100    46345.007 ±       37.999    B/op
//SelectBenchmark.client                                10000     4528.870 ±      927.183   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm            10000  4838856.987 ±       68.005    B/op
//
//SelectBenchmark.jdbc                                      1      136.829 ±       28.282   us/op
//SelectBenchmark.jdbc:·gc.alloc.rate.norm                  1     1492.178 ±        1.795    B/op
//SelectBenchmark.jdbc                                    100      269.950 ±       54.267   us/op
//SelectBenchmark.jdbc:·gc.alloc.rate.norm                100    48052.161 ±        3.890    B/op
//SelectBenchmark.jdbc                                  10000    13743.088 ±     3963.558   us/op
//SelectBenchmark.jdbc:·gc.alloc.rate.norm              10000  4488546.439 ±      231.620    B/op
//
//SelectBenchmark.referenceClient                           1       93.135 ±       27.792   us/op
//SelectBenchmark.referenceClient:·gc.alloc.rate.norm       1    10209.644 ±     8871.899    B/op
//SelectBenchmark.referenceClient                         100      165.087 ±       41.909   us/op
//SelectBenchmark.referenceClient:·gc.alloc.rate.norm     100    98612.903 ±   199166.349    B/op
//SelectBenchmark.referenceClient                       10000     5261.139 ±     1107.887   us/op
//SelectBenchmark.referenceClient:·gc.alloc.rate.norm   10000  9190418.672 ± 19769946.733    B/op
//
//SelectBenchmark.connection                                1       85.325 ±       11.463   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm            1    11760.055 ±        0.148    B/op
//SelectBenchmark.connection                              100      358.742 ±       36.729   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm          100   129392.232 ±        0.626    B/op
//SelectBenchmark.connection                            10000    36867.034 ±     3317.009   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm        10000  2677735.898 ±      134.961    B/op
@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class SingleSelectBenchmark {

	private TarantoolConnection connection;
	private org.tarantool.TarantoolClient referenceClient;
	private TarantoolClientSource clientSource;
	private TarantoolTemplate template;
	private Connection jdbcConnection;
	private int space;

	@Param({ "1", "100", "10000" })
	public int size;

	@Setup
	public void setup() throws Exception {
		SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
		referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
				new TarantoolClientConfig());
		connection = new TarantoolConnection(null, null, new Socket("localhost", 3301));
		clientSource = new TarantoolPooledClientSource("localhost", 3301, 1);

		template = new TarantoolTemplate(clientSource);
		jdbcConnection = new com.sopovs.moradanen.tarantool.jdbc.TarantoolConnection(new TarantoolClientImpl("localhost", 3301));
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
		List<Foo> result = new ArrayList<>();
		try (PreparedStatement pst = jdbcConnection.prepareStatement("SELECT * FROM JDBCBENCHMARK");
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
	public List<?> connection() {
		return connection.select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE, Iter.ALL.getValue());
	}

	@Benchmark
	public List<?> referenceClient() {
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

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(".*" + SingleSelectBenchmark.class.getSimpleName() + ".*")
				.addProfiler(GCProfiler.class).build();

		new Runner(opt).run();
	}

}
