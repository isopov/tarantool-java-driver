package com.sopovs.moradanen.tarantool.core;

public enum Op {
	PLUS("+"), MINUS("-"), AND("&"), OR("|"), XOR("^"), DELETE("#");

	private final String val;

	private Op(String val) {
		this.val = val;
	}

	public String getVal() {
		return val;
	}
}