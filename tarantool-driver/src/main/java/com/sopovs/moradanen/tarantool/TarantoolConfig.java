package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.Nullable;

public class TarantoolConfig {
    @Nullable
    private final String host;
    private final int port;
    @Nullable
    private final String username;
    @Nullable
    // TODO char[] ?
    private final String password;

    public TarantoolConfig(@Nullable String host, int port, @Nullable String username, @Nullable String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    @Nullable
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    String getPassword() {
        return password;
    }
}
