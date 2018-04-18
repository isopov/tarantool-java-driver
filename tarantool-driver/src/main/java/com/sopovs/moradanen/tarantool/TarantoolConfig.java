package com.sopovs.moradanen.tarantool;

public class TarantoolConfig {
    private final String host;
    private final int port;
    private final String username;
    // TODO char[] ?
    private final String password;

    public TarantoolConfig(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }
}
