package com.sopovs.moradanen.tarantool.benchmarks;

import com.sopovs.moradanen.tarantool.*;
import com.sopovs.moradanen.tarantool.core.Iter;
import org.openjdk.jmh.annotations.*;
import org.tarantool.TarantoolClientConfig;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//Benchmark                                        (type)  Mode  Cnt      Score     Error  Units
//ConcurrentSelectBenchmark.select        referenceClient  avgt   15    135.715 ±   0.819  us/op
//ConcurrentSelectBenchmark.select     pooledClientSource  avgt   15    181.730 ±   1.669  us/op
//ConcurrentSelectBenchmark.select            threadLocal  avgt   15    180.817 ±   1.310  us/op
//ConcurrentSelectBenchmark.selectAll     referenceClient  avgt   15  41187.509 ± 522.923  us/op
//ConcurrentSelectBenchmark.selectAll  pooledClientSource  avgt   15  13861.497 ± 181.352  us/op
//ConcurrentSelectBenchmark.selectAll         threadLocal  avgt   15  14099.695 ± 125.618  us/op

//
//
//Simulating network latency of 100 us with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 100usec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                     (type)  Mode  Cnt    Score     Error  Units
//ConcurrentSelectBenchmark.select     referenceClient  avgt   15  641.152 ± 160.129  us/op
//ConcurrentSelectBenchmark.select  pooledClientSource  avgt   15  314.228 ±   0.637  us/op
//ConcurrentSelectBenchmark.select         threadLocal  avgt   15  312.966 ±   1.134  us/op

//
//
//Simulating network latency of 1ms with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 1msec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                     (type)  Mode  Cnt     Score    Error  Units
//ConcurrentSelectBenchmark.select     referenceClient  avgt   15  5348.508 ± 69.763  us/op
//ConcurrentSelectBenchmark.select  pooledClientSource  avgt   15  2668.494 ± 42.226  us/op
//ConcurrentSelectBenchmark.select         threadLocal  avgt   15  2657.087 ± 53.612  us/op

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(16)
public class ConcurrentSelectBenchmark {

    private static final String THREAD_LOCAL = "threadLocal";
    private static final String POOLED_CLIENT_SOURCE = "pooledClientSource";
    private static final String REFERENCE_CLIENT = "referenceClient";
    int size = 10000;
    private org.tarantool.TarantoolClient referenceClient;
    private TarantoolClientSource clientSource;
    private ThreadLocal<TarantoolClient> threadLocalClient;
    private int space;

    @Param({REFERENCE_CLIENT, POOLED_CLIENT_SOURCE, THREAD_LOCAL})
    public String type;

    @Setup
    public void setup() throws Exception {
        switch (type) {
            case REFERENCE_CLIENT:
                SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
                TarantoolClientConfig tarantoolClientConfig = new TarantoolClientConfig();
                tarantoolClientConfig.username = "admin";
                tarantoolClientConfig.password = "javapass";
                referenceClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
                        tarantoolClientConfig);
                break;
            case POOLED_CLIENT_SOURCE:
                clientSource = new TarantoolPooledClientSource("localhost", 3301, "admin", "javapass", 16);
                break;
            case THREAD_LOCAL:
                threadLocalClient = ThreadLocal.withInitial(() -> new TarantoolClientImpl("localhost", "admin", "javapass"));
                break;
            default:
                throw new IllegalStateException();
        }

        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            client.evalFully("box.schema.space.create('javabenchmark')").close();
            client.evalFully(
                    "box.space.javabenchmark:create_index('primary', {type = 'hash', parts = {1, 'num'}})")
                    .close();
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

    String clientSource() {
        try (TarantoolClient client = clientSource.getClient()) {
            return fromClient(client);
        }
    }

    List<String> threadLocalAll() {
        return fromClientAll(threadLocalClient.get());
    }

    List<String> clientSourceAll() {
        try (TarantoolClient client = clientSource.getClient()) {
            return fromClientAll(client);
        }
    }

    String threadLocal() {
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

    String referenceClient() {
        int key = ThreadLocalRandom.current().nextInt(size);
        List<?> result = referenceClient.syncOps().select(space, 0, Collections.singletonList(key), 0,
                Integer.MAX_VALUE, Iter.EQ.getValue());
        if (result.size() != 1) {
            throw new IllegalStateException();
        }
        return (String) ((List<?>) result.get(0)).get(1);
    }

    List<String> referenceClientAll() {
        List<?> result = referenceClient.syncOps().select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE,
                Iter.ALL.getValue());
        if (result.size() != size) {
            throw new IllegalStateException();
        }
        return result.stream()
                .map(List.class::cast)
                .map(row -> row.get(1))
                .map(String.class::cast)
                .collect(Collectors.toList());
    }

    @TearDown
    public void tearDown() {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            client.evalFully("box.space.javabenchmark:drop()").close();
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
}
