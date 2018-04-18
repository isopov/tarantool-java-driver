package com.sopovs.moradanen.tarantool.benchmarks;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.buffer.MessageBuffer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

//Benchmark                                  (size)  Mode  Cnt        Score       Error  Units
//MessageBufferPackerBenchmark.simple            10  avgt   15       40.960 ±     0.405  ns/op
//MessageBufferPackerBenchmark.bufferList        10  avgt   15       34.575 ±     0.476  ns/op
//MessageBufferPackerBenchmark.bufferList2       10  avgt   15       16.933 ±     0.721  ns/op
//
//
//MessageBufferPackerBenchmark.simple           100  avgt   15      125.125 ±    10.445  ns/op
//MessageBufferPackerBenchmark.bufferList       100  avgt   15       90.574 ±     1.605  ns/op
//MessageBufferPackerBenchmark.bufferList2      100  avgt   15       16.951 ±     1.308  ns/op
//
//MessageBufferPackerBenchmark.simple          1000  avgt   15      712.290 ±    32.999  ns/op
//MessageBufferPackerBenchmark.bufferList      1000  avgt   15      659.406 ±   159.251  ns/op
//MessageBufferPackerBenchmark.bufferList2     1000  avgt   15       18.063 ±     1.479  ns/op
//
//MessageBufferPackerBenchmark.simple         10000  avgt   15     8666.244 ±   259.526  ns/op
//MessageBufferPackerBenchmark.bufferList     10000  avgt   15     6092.441 ±  1714.734  ns/op
//MessageBufferPackerBenchmark.bufferList2    10000  avgt   15       44.079 ±     2.703  ns/op
//
//MessageBufferPackerBenchmark.simple        100000  avgt   15   137976.456 ± 20336.953  ns/op
//MessageBufferPackerBenchmark.bufferList    100000  avgt   15    51856.490 ±  6159.722  ns/op
//MessageBufferPackerBenchmark.bufferList2   100000  avgt   15      271.133 ±    15.437  ns/op
//
//MessageBufferPackerBenchmark.simple       1000000  avgt   15  2419730.303 ± 67492.748  ns/op
//MessageBufferPackerBenchmark.bufferList   1000000  avgt   15   554013.945 ± 41255.504  ns/op
//MessageBufferPackerBenchmark.bufferList2  1000000  avgt   15     3049.956 ±   186.901  ns/op

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class MessageBufferPackerBenchmark {

    private final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

    @Param({"10", "100", "1000", "10000", "100000", "1000000"})
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
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

}
