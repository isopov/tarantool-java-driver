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
