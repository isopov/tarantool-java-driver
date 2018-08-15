package com.sopovs.moradanen.tarantool;

import org.junit.jupiter.api.Test;

class TarantoolAsyncClientTest {


    @Test
    void testConnect() {
        //noinspection EmptyTryBlock
        try (TarantoolAsyncClient ignored = new TarantoolAsyncClient("localhost")) {
        }
    }

    @Test
    void testPing() throws Exception {
        try (TarantoolAsyncClient client = new TarantoolAsyncClient("localhost")) {
            client.ping().get();
        }
    }

    @Test
    void test2Ping() throws Exception {
        try (TarantoolAsyncClient client = new TarantoolAsyncClient("localhost")) {
            client.ping().get();
            client.ping().get();
        }
    }

}