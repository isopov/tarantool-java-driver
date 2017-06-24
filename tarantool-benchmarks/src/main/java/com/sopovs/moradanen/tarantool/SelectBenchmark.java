package com.sopovs.moradanen.tarantool;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
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

//Benchmark                                                         (size)  Mode  Cnt         Score          Error   Units
//SelectBenchmark.client                                                 1  avgt    5        44.097 ±       13.222   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                             1  avgt    5       696.045 ±        1.530    B/op
//SelectBenchmark.client                                               100  avgt    5        89.070 ±       34.241   us/op
//SelectBenchmark.client:·gc.alloc.rate                                100  avgt    5       298.388 ±      108.160  MB/sec
//SelectBenchmark.client:·gc.alloc.rate.norm                           100  avgt    5     41526.581 ±        5.355    B/op
//SelectBenchmark.client                                             10000  avgt    5      3520.862 ±      275.662   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                         10000  avgt    5   4359367.929 ±        9.720    B/op
//
//SelectBenchmark.referenceClient                                        1  avgt    5        80.325 ±       17.750   us/op
//SelectBenchmark.referenceClient:·gc.alloc.rate.norm                    1  avgt    5     10209.649 ±     8871.924    B/op
//SelectBenchmark.referenceClient                                      100  avgt    5       138.650 ±       30.248   us/op
//SelectBenchmark.referenceClient:·gc.alloc.rate.norm                  100  avgt    5     98612.892 ±   199166.333    B/op
//SelectBenchmark.referenceClient                                    10000  avgt    5      4769.988 ±      270.897   us/op
//SelectBenchmark.referenceClient:·gc.alloc.rate.norm                10000  avgt    5   9190430.678 ± 19769972.934    B/op
//
//SelectBenchmark.connection                                             1  avgt    5        65.622 ±        6.248   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                         1  avgt    5     11760.048 ±        0.115    B/op
//SelectBenchmark.connection                                           100  avgt    5       339.908 ±       10.309   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                       100  avgt    5    129392.217 ±        0.552    B/op
//SelectBenchmark.connection                                         10000  avgt    5     35564.391 ±     2027.553   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                     10000  avgt    5  12677737.072 ±      134.566    B/op

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class SelectBenchmark {

	private TarantoolConnection connection;
	private org.tarantool.TarantoolClient referenceClient;
	private TarantoolClientSource clientSource;
	private TarantoolTemplate template;
	private int space;

	@Param({ "1", "100", "10000" })
	public int size;

	@Setup
	public void setup() throws Exception {
		SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
		referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
				new TarantoolClientConfig());
		connection = new TarantoolConnection(null, null, new Socket("localhost", 3301));

		clientSource = new TarantoolSingleClientSource("localhost", 3301);
		template = new TarantoolTemplate(clientSource);

		try (TarantoolClient client = clientSource.getClient()) {
			client.evalFully("box.schema.space.create('javabenchmark')").consume();
			client.evalFully(
					"box.space.javabenchmark:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})")
					.consume();
			space = client.space("javabenchmark");
			for (int i = 0; i < size; i++) {
				client.insert(space);
				client.setInt(i);
				client.setString("FooBar" + i);
				client.addBatch();
			}
			client.executeBatch();
		}

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
	public void tearDown() {
		try (TarantoolClient client = clientSource.getClient()) {
			client.evalFully("box.space.javabenchmark:drop()").consume();
		}
		connection.close();
		referenceClient.close();
		clientSource.close();
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(".*" + SelectBenchmark.class.getSimpleName() + ".*")
				.addProfiler(GCProfiler.class).build();

		new Runner(opt).run();
	}

}
