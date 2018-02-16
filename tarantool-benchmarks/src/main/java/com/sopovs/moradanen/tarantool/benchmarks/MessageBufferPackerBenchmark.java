package com.sopovs.moradanen.tarantool.benchmarks;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.buffer.MessageBuffer;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

//Benchmark                                    (size)  Mode  Cnt        Score        Error   Units
//MessageBufferPackerBenchmark.simple              10  avgt   15       39.023 ±      2.448   ns/op
//MessageBufferPackerBenchmark.bufferList          10  avgt   15       35.140 ±      0.607   ns/op
//MessageBufferPackerBenchmark.bufferList2         10  avgt   15       18.644 ±      0.638   ns/op
//
//MessageBufferPackerBenchmark.simple             100  avgt   15       90.248 ±      8.005   ns/op
//MessageBufferPackerBenchmark.bufferList         100  avgt   15      121.054 ±     41.041   ns/op
//MessageBufferPackerBenchmark.bufferList2        100  avgt   15       17.891 ±      0.219   ns/op
//
//MessageBufferPackerBenchmark.simple            1000  avgt   15      513.832 ±     42.584   ns/op
//MessageBufferPackerBenchmark.bufferList        1000  avgt   15      643.427 ±    210.402   ns/op
//MessageBufferPackerBenchmark.bufferList2       1000  avgt   15       18.333 ±      0.975   ns/op
//
//MessageBufferPackerBenchmark.simple           10000  avgt   15     6788.958 ±    849.847   ns/op
//MessageBufferPackerBenchmark.bufferList       10000  avgt   15     4924.296 ±    370.962   ns/op
//MessageBufferPackerBenchmark.bufferList2      10000  avgt   15       52.053 ±      1.067   ns/op
//
//MessageBufferPackerBenchmark.simple          100000  avgt   15   109543.757 ±  20722.096   ns/op
//MessageBufferPackerBenchmark.bufferList      100000  avgt   15    49238.046 ±   2772.679   ns/op
//MessageBufferPackerBenchmark.bufferList2     100000  avgt   15      354.618 ±      4.344   ns/op
//
//MessageBufferPackerBenchmark.simple         1000000  avgt   15  1604065.262 ±  41528.849   ns/op
//MessageBufferPackerBenchmark.bufferList     1000000  avgt   15   760960.681 ± 284783.958   ns/op
//MessageBufferPackerBenchmark.bufferList2    1000000  avgt   15     3795.162 ±    188.526   ns/op

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class MessageBufferPackerBenchmark {

	private final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

	@Param({ "10", "100", "1000", "10000", "100000", "1000000" })
	public int size;

	@Setup
	public void setup() throws IOException {
		Random r = new Random(42L);
		for (int i = 0; i < size; i++) {
			packer.packInt(r.nextInt());
		}
	}

	@Benchmark
	public void simple(Blackhole bh) {
		bh.consume(packer.getBufferSize());
		bh.consume(packer.toByteArray());
	}

	@Benchmark
	public void bufferList(Blackhole bh) {
		List<MessageBuffer> bufferList = packer.toBufferList();
		writeSize(bufferList, bh);
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			bh.consume(messageBuffer.toByteArray());
		}
	}

	@Benchmark
	public void bufferList2(Blackhole bh) {
		List<MessageBuffer> bufferList = packer.toBufferList();
		writeSize(bufferList, bh);
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			bh.consume(messageBuffer.array());
		}
	}

	private void writeSize(List<MessageBuffer> bufferList, Blackhole bh) {
		int size = 0;
		for (int i = 0; i < bufferList.size(); i++) {
			MessageBuffer messageBuffer = bufferList.get(i);
			size += messageBuffer.size();
		}
		bh.consume(size);
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
				.include(".*" + MessageBufferPackerBenchmark.class.getSimpleName() + ".*")
				.addProfiler(GCProfiler.class).build();

		new Runner(opt).run();
	}

}
