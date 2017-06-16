package com.sopovs.moradanen.tarantool;

public final class Util {
	private Util() {
	}

	// header
	public static final int KEY_CODE = 0x00;

	public static final int CODE_SELECT = 1;
	public static final int CODE_INSERT = 2;
	public static final int CODE_REPLACE = 3;
	public static final int CODE_UPDATE = 4;
	public static final int CODE_DELETE = 5;
	public static final int CODE_OLD_CALL = 6;
	public static final int CODE_AUTH = 7;
	public static final int CODE_EVAL = 8;
	public static final int CODE_UPSERT = 9;
	public static final int CODE_CALL = 10;
	public static final int CODE_PING = 64;
	public static final int CODE_SUBSCRIBE = 66;

	public static final int KEY_SYNC = 0x01;
	public static final int KEY_SCHEMA_ID = 0x05;
	// body
	public static final int KEY_SPACE = 0x10;
	public static final int KEY_INDEX = 0x11;
	public static final int KEY_LIMIT = 0x12;
	public static final int KEY_OFFSET = 0x13;
	public static final int KEY_ITERATOR = 0x14;
	public static final int KEY_KEY = 0x20;
	public static final int KEY_TUPLE = 0x21;
	public static final int KEY_FUNCTION = 0x22;
	public static final int KEY_USER_NAME = 0x23;
	public static final int KEY_EXPRESSION = 0x27;
	public static final int KEY_UPSERT_OPS = 0x28;
	public static final int KEY_DATA = 0x30;
	public static final int KEY_ERROR = 0x31;
}
