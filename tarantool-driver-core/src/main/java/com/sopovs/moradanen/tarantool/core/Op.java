package com.sopovs.moradanen.tarantool.core;

public enum Op {
	DELETE("#"), INSERT("!"), ASSIGN("=");

	private final String val;

	private Op(String val) {
		this.val = val;
	}

	public String getVal() {
		return val;
	}
}
