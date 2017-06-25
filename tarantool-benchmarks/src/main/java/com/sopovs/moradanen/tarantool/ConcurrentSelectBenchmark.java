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
import org.openjdk.jmh.annotations.Param;
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


//Benchmark                                     (type)  Mode  Cnt    Score    Error  Units
//ConcurrentSelectBenchmark.select     referenceClient  avgt   15  151.573 ±  1.951  us/op
//ConcurrentSelectBenchmark.select  pooledClientSource  avgt   15  161.725 ± 10.787  us/op
//ConcurrentSelectBenchmark.select         threadLocal  avgt   15  152.732 ±  1.209  us/op
//
//
//Simulating network latency of 100 us with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 100usec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                     (type)  Mode  Cnt    Score    Error  Units
//ConcurrentSelectBenchmark.select     referenceClient  avgt   15  575.669 ± 11.640  us/op
//ConcurrentSelectBenchmark.select  pooledClientSource  avgt   15  320.201 ±  4.336  us/op
//ConcurrentSelectBenchmark.select         threadLocal  avgt   15  317.752 ±  4.839  us/op
//
//
//Simulating network latency of 1ms with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 1msec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                     (type)  Mode  Cnt     Score    Error  Units
//ConcurrentSelectBenchmark.select     referenceClient  avgt   15  4476.049 ± 68.609  us/op
//ConcurrentSelectBenchmark.select  pooledClientSource  avgt   15  2167.582 ± 17.756  us/op
//ConcurrentSelectBenchmark.select         threadLocal  avgt   15  2164.908 ± 13.743  us/op


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
	private ThreadLocal<TarantoolClient> threadLocalClient;
	private int space;

	@Param({ "referenceClient", "pooledClientSource", "threadLocal" })
	public String type;

	@Setup
	public void setup() throws Exception {
		switch (type) {
		case "referenceClient":
			SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
			referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
					new TarantoolClientConfig());
			break;
		case "pooledClientSource":
			clientSource = new TarantoolPooledClientSource("localhost", 3301, 16);
			break;
		case "threadLocal":
			threadLocalClient = ThreadLocal.withInitial(() -> new TarantoolClientImpl("localhost"));
			break;
		default:
			throw new IllegalStateException();
		}

		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
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
	public String select() {
		switch (type) {
		case "referenceClient":
			return referenceClient();
		case "pooledClientSource":
			return clientSource();
		case "threadLocal":
			return threadLocal();
		default:
			throw new IllegalStateException();
		}
	}

	protected String clientSource() {
		try (TarantoolClient client = clientSource.getClient()) {
			return fromClient(client);
		}
	}

	protected String threadLocal() {
		return fromClient(threadLocalClient.get());
	}

	private String fromClient(TarantoolClient client) {
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

	protected String referenceClient() {
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
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.evalFully("box.space.javabenchmark:drop()").consume();
		}
		switch (type) {
		case "referenceClient":
			referenceClient.close();
			break;
		case "pooledClientSource":
			clientSource.close();
			break;
		case "threadLocal":
			// TODO leave to gc?
			break;
		default:
			throw new IllegalStateException();
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder().include(".*" + ConcurrentSelectBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(opt).run();
	}

}
