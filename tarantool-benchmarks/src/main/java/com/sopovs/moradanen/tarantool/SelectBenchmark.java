package com.sopovs.moradanen.tarantool;

import java.net.Socket;
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
import org.tarantool.TarantoolConnection;

//Benchmark                                                    (size)  Mode  Cnt        Score         Error   Units
//SelectBenchmark.client                                            1  avgt   15       17.889 ±       2.666   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                        1  avgt   15      290.339 ±       8.934    B/op
//SelectBenchmark.client                                           10  avgt   15       17.034 ±       0.090   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                       10  avgt   15      889.304 ±      16.064    B/op
//SelectBenchmark.client                                          100  avgt   15       23.069 ±       0.394   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                      100  avgt   15     6663.479 ±      19.751    B/op
//SelectBenchmark.client                                         1000  avgt   15       74.478 ±       2.829   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                     1000  avgt   15    53605.639 ±    8346.383    B/op
//SelectBenchmark.client                                        10000  avgt   15      614.299 ±      16.483   us/op
//SelectBenchmark.client:·gc.alloc.rate.norm                    10000  avgt   15   480589.734 ±       2.191    B/op
//
//SelectBenchmark.connection                                        1  avgt   15       28.267 ±       0.454   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                    1  avgt   15    11328.016 ±       0.008    B/op
//SelectBenchmark.connection                                       10  avgt   15       34.625 ±       0.335   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                   10  avgt   15    18128.020 ±       0.010    B/op
//SelectBenchmark.connection                                      100  avgt   15      101.093 ±       2.168   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                  100  avgt   15    86192.057 ±       0.029    B/op
//SelectBenchmark.connection                                     1000  avgt   15     1273.523 ±     156.929   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                 1000  avgt   15   819332.096 ±       7.482    B/op
//SelectBenchmark.connection                                    10000  avgt   15    14056.247 ±    2883.599   us/op
//SelectBenchmark.connection:·gc.alloc.rate.norm                10000  avgt   15  8199336.207 ±       4.124    B/op

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class SelectBenchmark {

	private TarantoolConnection connection;
	private TarantoolClient client;
	private int space;

	@Param({ "1", "10", "100", "1000", "10000" })
	public int size;

	@Setup
	public void setup() throws Exception {
		connection = new TarantoolConnection(null, null, new Socket("localhost", 3301));
		client = new TarantoolClientImpl("localhost");

		client.evalFully("box.schema.space.create('javabenchmark')").consume();
		client.evalFully("box.space.javabenchmark:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})")
				.consume();
		space = client.space("javabenchmark");
		for (int i = 0; i < size; i++) {
			client.insert(space);
			client.setInt(i);
			client.addBatch();
		}
		client.executeBatch();

	}

	@Benchmark
	public int client() {
		client.selectAll(space);
		Result select = client.execute();
		select.consume();
		return select.getSize();
	}

	@Benchmark
	public List<?> connection() {
		return connection.select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE, Iter.ALL.getValue());
	}

	@TearDown
	public void tearDown() {
		client.evalFully("box.space.javabenchmark:drop()").consume();
		connection.close();
		client.close();
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(".*" + SelectBenchmark.class.getSimpleName() + ".*")
				.addProfiler(GCProfiler.class)
				.build();

		new Runner(opt).run();
	}

}
