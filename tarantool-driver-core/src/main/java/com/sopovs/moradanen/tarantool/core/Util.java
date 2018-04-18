package com.sopovs.moradanen.tarantool.core;

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
    // SQL
    public static final int CODE_EXECUTE = 11;
    public static final int CODE_NOP = 12;
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
    // SQL
    public static final int KEY_FIELD_NAME = 0x29;
    public static final int KEY_METADATA = 0x32;
    public static final int KEY_SQL_TEXT = 0x40;
    public static final int KEY_SQL_BIND = 0x41;
    public static final int KEY_SQL_OPTIONS = 0x42;
    public static final int KEY_SQL_INFO = 0x43;
    public static final int KEY_SQL_ROW_COUNT = 0x44;


    public static final int SPACE_SCHEMA = 272;
    public static final int SPACE_SPACE = 280;
    public static final int SPACE_INDEX = 288;
    public static final int SPACE_FUNC = 296;
    public static final int SPACE_VSPACE = 281;
    public static final int SPACE_VINDEX = 289;
    public static final int SPACE_VFUNC = 297;
    public static final int SPACE_USER = 304;
    public static final int SPACE_PRIV = 312;
    public static final int SPACE_CLUSTER = 320;

    public static final int INDEX_SPACE_PRIMARY = 0;
    public static final int INDEX_SPACE_NAME = 2;
    public static final int INDEX_INDEX_PRIMARY = 0;
    public static final int INDEX_INDEX_NAME = 2;
}
