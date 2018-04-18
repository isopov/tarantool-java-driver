package com.sopovs.moradanen.tarantool;

public class TarantoolNewClientSource implements TarantoolClientSource {

    private final String host;
    private final int port;

    public TarantoolNewClientSource(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public TarantoolClient getClient() {
        return new TarantoolClientImpl(host, port);
    }

    @Override
    public void close() {
        // TODO prevent creating new after close?
    }
}
