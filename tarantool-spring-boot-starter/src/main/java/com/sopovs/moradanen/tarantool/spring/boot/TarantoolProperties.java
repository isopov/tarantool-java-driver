package com.sopovs.moradanen.tarantool.spring.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.tarantool")
public class TarantoolProperties {
    /**
     * Default port used when the configured port is {@code null}.
     */
    public static final int DEFAULT_PORT = 3301;

    /**
     * Tarantool server host.
     */
    private String host;

    /**
     * Tarantool server port.
     */
    private int port = DEFAULT_PORT;

    /**
     * Login user of the tarantool server.
     */
    private String username;

    /**
     * Login password of the tarantool server.
     */
    //TODO char[] ?
    private String password;

    /**
     * TarantoolPooledClientSource size.
     */
    // TODO Consider setting default to 1 that is good for super simple
    // samples, but encourages to redefine this value for any serious
    // application
    private int poolSize = 10;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }
}
