package com.sopovs.moradanen.tarantool.core;

public enum IntOp {
    PLUS("+"), MINUS("-"), AND("&"), OR("|"), XOR("^"), DELETE("#"), INSERT("!"), ASSIGN("=");

    private final String val;

    IntOp(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }
}