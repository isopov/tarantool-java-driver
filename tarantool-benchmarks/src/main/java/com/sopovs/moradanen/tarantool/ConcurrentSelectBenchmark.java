package com.sopovs.moradanen.tarantool;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.tarantool.TarantoolClientConfig;

//Benchmark                                  Mode  Cnt      Score    Error  Units
//ConcurrentSelectBenchmark.client           avgt   15  163.153 ± 6.217  us/op
//ConcurrentSelectBenchmark.referenceClient  avgt   15  155.670 ± 2.891  us/op

//Simulating network latency of 1ms with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 1msec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                  Mode  Cnt     Score    Error  Units
//ConcurrentSelectBenchmark.client           avgt   15  2157.937 ± 11.086  us/op
//ConcurrentSelectBenchmark.referenceClient  avgt   15  4357.253 ± 33.864  us/op

//Simulating network latency of 10ms with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 10msec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                  Mode  Cnt      Score    Error  Units
//ConcurrentSelectBenchmark.client           avgt   15  20352.330 ± 25.479  us/op
//ConcurrentSelectBenchmark.referenceClient  avgt   15  40481.864 ± 68.298  us/op

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(16)
public class ConcurrentSelectBenchmark {

	public int size = 1000;
	private org.tarantool.TarantoolClient referenceClient;
	private TarantoolClientSource clientSource;
	private int space;

	@Setup
	public void setup() throws Exception {
		SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
		referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
				new TarantoolClientConfig());

		clientSource = new TarantoolPooledClientSource("localhost", 3301, 16);

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
	public String client() {
		try (TarantoolClient client = clientSource.getClient()) {
			int key = ThreadLocalRandom.current().nextInt(size);
			client.select(space, 0);
			client.setInt(key);
			Result result = client.execute();
			if (result.getSize() != 1) {
				throw new IllegalStateException();
			}
			result.next();
			return result.getString(1);

		}
	}

	@Benchmark
	public String referenceClient() {
		int key = ThreadLocalRandom.current().nextInt(size);
		List<?> result = referenceClient.syncOps().select(space, 0, Collections.singletonList(key), 0,
				Integer.MAX_VALUE, Iter.EQ.getValue());
		if (result.size() != 1) {
			throw new IllegalStateException();
		}
		return (String) ((List<?>) result.get(0)).get(1);
	}

	@TearDown
	public void tearDown() {
		try (TarantoolClient client = clientSource.getClient()) {
			client.evalFully("box.space.javabenchmark:drop()").consume();
		}
		referenceClient.close();
		clientSource.close();
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(".*" + ConcurrentSelectBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(opt).run();
	}

}
