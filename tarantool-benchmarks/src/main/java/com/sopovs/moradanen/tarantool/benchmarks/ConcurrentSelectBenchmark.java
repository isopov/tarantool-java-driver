package com.sopovs.moradanen.tarantool.benchmarks;

import com.sopovs.moradanen.tarantool.*;
import com.sopovs.moradanen.tarantool.core.Iter;
import org.openjdk.jmh.annotations.*;
import org.tarantool.TarantoolClientConfig;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//With the following docker-compose.yml
//version: '2'
//
//services:
//  tarantool1:
//    image: tarantool/tarantool:2
//    environment:
//      TARANTOOL_REPLICATION: "tarantool1,tarantool2,tarantool3,tarantool4"
//      TARANTOOL_USER_NAME: "admin"
//      TARANTOOL_USER_PASSWORD: "javapass"
//    networks:
//      - mynet
//    ports:
//      - "3301:3301"
//
//  tarantool2:
//    image: tarantool/tarantool:2
//    environment:
//      TARANTOOL_REPLICATION: "tarantool1,tarantool2,tarantool3,tarantool4"
//      TARANTOOL_USER_NAME: "admin"
//      TARANTOOL_USER_PASSWORD: "javapass"
//    networks:
//      - mynet
//    ports:
//      - "3302:3301"
//
//  tarantool3:
//    image: tarantool/tarantool:2
//    environment:
//      TARANTOOL_REPLICATION: "tarantool1,tarantool2,tarantool3,tarantool4"
//      TARANTOOL_USER_NAME: "admin"
//      TARANTOOL_USER_PASSWORD: "javapass"
//    networks:
//      - mynet
//    ports:
//      - "3303:3301"
//
//  tarantool4:
//    image: tarantool/tarantool:2
//    environment:
//      TARANTOOL_REPLICATION: "tarantool1,tarantool2,tarantool3,tarantool4"
//      TARANTOOL_USER_NAME: "admin"
//      TARANTOOL_USER_PASSWORD: "javapass"
//    networks:
//      - mynet
//    ports:
//      - "3304:3301"
//networks:
//  mynet:
//    driver: bridge
//
//
//
//Benchmark                                                  (type)      Score      Error  Units
//ConcurrentSelectBenchmark.select                   upstreamClient    147.140 ±    0.985  us/op
//ConcurrentSelectBenchmark.select               pooledClientSource    212.533 ±    7.965  us/op
//ConcurrentSelectBenchmark.select     pooledReplicatedClientSource    238.277 ±    3.076  us/op
//ConcurrentSelectBenchmark.select                      threadLocal    211.266 ±    5.333  us/op
//ConcurrentSelectBenchmark.selectAll                upstreamClient  43922.911 ± 3978.287  us/op
//ConcurrentSelectBenchmark.selectAll            pooledClientSource  16110.452 ± 1272.221  us/op
//ConcurrentSelectBenchmark.selectAll  pooledReplicatedClientSource  15559.695 ± 2023.787  us/op
//ConcurrentSelectBenchmark.selectAll                   threadLocal  17789.294 ± 2104.801  us/op
//
//Simulating network latency of 100 us with
//`sudo tc qdisc add dev lo root handle 1:0 netem delay 100usec`
//(To restore `sudo tc qdisc del dev lo root`)
//Benchmark                                                  (type)      Score      Error  Units
//ConcurrentSelectBenchmark.select                   upstreamClient    612.111 ±   21.962  us/op
//ConcurrentSelectBenchmark.select               pooledClientSource    367.274 ±   18.223  us/op
//ConcurrentSelectBenchmark.select     pooledReplicatedClientSource    391.079 ±    3.983  us/op
//ConcurrentSelectBenchmark.select                      threadLocal    357.235 ±    5.318  us/op
//ConcurrentSelectBenchmark.selectAll                upstreamClient  49554.083 ± 2024.916  us/op
//ConcurrentSelectBenchmark.selectAll            pooledClientSource  17479.365 ±  691.103  us/op
//ConcurrentSelectBenchmark.selectAll  pooledReplicatedClientSource  18850.286 ±  709.923  us/op
//ConcurrentSelectBenchmark.selectAll                   threadLocal  17089.364 ±  373.411  us/op

@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(16)
public class ConcurrentSelectBenchmark {
    static final String THREAD_LOCAL = "threadLocal";
    static final String POOLED_CLIENT_SOURCE = "pooledClientSource";
    static final String POOLED_REPL_CLIENT_SOURCE = "pooledReplicatedClientSource";
    static final String UPSTREAM_CLIENT = "upstreamClient";
    int size = 10000;
    private org.tarantool.TarantoolClient upstreamClient;
    private TarantoolClientSource clientSource;
    private ThreadLocal<TarantoolClient> threadLocalClient;
    private int space;

    @Param({UPSTREAM_CLIENT, POOLED_CLIENT_SOURCE, POOLED_REPL_CLIENT_SOURCE, THREAD_LOCAL})
    public String type;

    @Setup
    public void setup() throws Exception {
        switch (type) {
            case UPSTREAM_CLIENT:
                SocketChannel referenceClientChannel = SocketChannel.open(new InetSocketAddress("localhost", 3301));
                TarantoolClientConfig tarantoolClientConfig = new TarantoolClientConfig();
                tarantoolClientConfig.username = "admin";
                tarantoolClientConfig.password = "javapass";
                upstreamClient = new org.tarantool.TarantoolClientImpl((r, e) -> referenceClientChannel,
                        tarantoolClientConfig);
                break;
            case POOLED_CLIENT_SOURCE:
                clientSource = new TarantoolPooledClientSource("localhost", 3301, "admin", "javapass", 16);
                break;
            case POOLED_REPL_CLIENT_SOURCE:
                clientSource = new TarantoolPooledClientSource(new HashMap<TarantoolConfig, Integer>() {{
                    put(new TarantoolConfig("localhost", 3301, "admin", "javapass"), 4);
                    put(new TarantoolConfig("localhost", 3302, "admin", "javapass"), 4);
                    put(new TarantoolConfig("localhost", 3303, "admin", "javapass"), 4);
                    put(new TarantoolConfig("localhost", 3304, "admin", "javapass"), 4);
                }});
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
        if (POOLED_REPL_CLIENT_SOURCE.equals(type)) {
            Thread.sleep(1000L);
        }
    }

    @Benchmark
    public String select() {
        switch (type) {
            case UPSTREAM_CLIENT:
                return referenceClient();
            case POOLED_CLIENT_SOURCE:
            case POOLED_REPL_CLIENT_SOURCE:
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
            case UPSTREAM_CLIENT:
                return referenceClientAll();
            case POOLED_CLIENT_SOURCE:
            case POOLED_REPL_CLIENT_SOURCE:
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
        List<?> result = upstreamClient.syncOps().select(space, 0, Collections.singletonList(key), 0,
                Integer.MAX_VALUE, Iter.EQ.getValue());
        if (result.size() != 1) {
            throw new IllegalStateException();
        }
        return (String) ((List<?>) result.get(0)).get(1);
    }

    List<String> referenceClientAll() {
        List<?> result = upstreamClient.syncOps().select(space, 0, Collections.emptyList(), 0, Integer.MAX_VALUE,
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
    public void tearDown() throws InterruptedException {
        try (TarantoolClient client = new TarantoolClientImpl("localhost", "admin", "javapass")) {
            client.evalFully("box.space.javabenchmark:drop()").close();
        }
        switch (type) {
            case UPSTREAM_CLIENT:
                upstreamClient.close();
                break;
            case POOLED_REPL_CLIENT_SOURCE:
                Thread.sleep(5000L);
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
