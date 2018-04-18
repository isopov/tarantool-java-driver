package com.sopovs.moradanen.tarantool.core;

public class TarantoolException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TarantoolException(String message, Throwable cause) {
        super(message, cause);
    }

    public TarantoolException(String message) {
        super(message);
    }

    public TarantoolException(Throwable cause) {
        super(cause);
    }
}
