package com.sopovs.moradanen.tarantool.benchmarks;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

import com.sopovs.moradanen.tarantool.Result;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.TarantoolClientImpl;
import com.sopovs.moradanen.tarantool.TarantoolClientSource;
import com.sopovs.moradanen.tarantool.TarantoolPooledClientSource;
import com.sopovs.moradanen.tarantool.core.Iter;

//Benchmark                                        (type)  Mode  Cnt      Score      Error  Units
//ConcurrentSelectBenchmark.select        referenceClient  avgt   15    155.582 ±    3.293  us/op
//ConcurrentSelectBenchmark.select     pooledClientSource  avgt   15    166.061 ±    8.467  us/op
//ConcurrentSelectBenchmark.select            threadLocal  avgt   15    154.984 ±    1.354  us/op
//ConcurrentSelectBenchmark.selectAll     referenceClient  avgt   15  60446.579 ± 1454.584  us/op
//ConcurrentSelectBenchmark.selectAll  pooledClientSource  avgt   15  23729.235 ±  580.103  us/op
//ConcurrentSelectBenchmark.selectAll         threadLocal  avgt   15  23934.715 ±  762.801  us/op
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

	private static final String THREAD_LOCAL = "threadLocal";
	private static final String POOLED_CLIENT_SOURCE = "pooledClientSource";
	private static final String REFERENCE_CLIENT = "referenceClient";
	public int size = 10000;
	private org.tarantool.TarantoolClient referenceClient;
	private TarantoolClientSource clientSource;
	private ThreadLocal<TarantoolClient> threadLocalClient;
	private int space;

	@Param({ REFERENCE_CLIENT, POOLED_CLIENT_SOURCE, THREAD_LOCAL })
	public String type;

	@Setup
	public void setup() throws Exception {
		switch (type) {
		case REFERENCE_CLIENT:
			SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
			referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
					new TarantoolClientConfig());
			break;
		case POOLED_CLIENT_SOURCE:
			clientSource = new TarantoolPooledClientSource("localhost", 3301, 16);
			break;
		case THREAD_LOCAL:
			threadLocalClient = ThreadLocal.withInitial(() -> new TarantoolClientImpl("localhost"));
			break;
		default:
			throw new IllegalStateException();
		}

		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.evalFully("box.schema.space.create('javabenchmark')").consume();
			client.evalFully(
					"box.space.javabenchmark:create_index('primary', {type = 'hash', parts = {1, 'num'}})")
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
		case REFERENCE_CLIENT:
			return referenceClient();
		case POOLED_CLIENT_SOURCE:
			return clientSource();
		case THREAD_LOCAL:
			return threadLocal();
		default:
			throw new IllegalStateException();
		}
	}

	@Benchmark
	public List<String> selectAll() {
		switch (type) {
		case REFERENCE_CLIENT:
			return referenceClientAll();
		case POOLED_CLIENT_SOURCE:
			return clientSourceAll();
		case THREAD_LOCAL:
			return threadLocalAll();
		default:
			throw new IllegalStateException();
		}
	}

	protected String clientSource() {
		try (TarantoolClient client = clientSource.getClient()) {
			return fromClient(client);
		}
	}

	protected List<String> threadLocalAll() {
		return fromClientAll(threadLocalClient.get());
	}

	protected List<String> clientSourceAll() {
		try (TarantoolClient client = clientSource.getClient()) {
			return fromClientAll(client);
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

	private List<String> fromClientAll(TarantoolClient client) {
		client.selectAll(space);
		Result select = client.execute();
		if (select.getSize() != size) {
			throw new IllegalStateException();
		}
		List<String> result = new ArrayList<>(select.getSize());
		while (select.next()) {
			result.add(select.getString(1));
		}
		return result;
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

	protected List<String> referenceClientAll() {
		List<?> result = referenceClient.syncOps().select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE,
				Iter.ALL.getValue());
		if (result.size() != size) {
			throw new IllegalStateException();
		}
		return result.stream().map(List.class::cast).map(row -> row.get(1)).map(String.class::cast)
				.collect(Collectors.toList());
	}

	@TearDown
	public void tearDown() {
		try (TarantoolClient client = new TarantoolClientImpl("localhost")) {
			client.evalFully("box.space.javabenchmark:drop()").consume();
		}
		switch (type) {
		case REFERENCE_CLIENT:
			referenceClient.close();
			break;
		case POOLED_CLIENT_SOURCE:
			clientSource.close();
			break;
		case THREAD_LOCAL:
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
