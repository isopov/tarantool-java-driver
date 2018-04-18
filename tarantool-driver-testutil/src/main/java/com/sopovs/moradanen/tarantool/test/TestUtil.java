package com.sopovs.moradanen.tarantool.test;

public class TestUtil {

    public static String getEnvTarantoolVersion() {
        String version = System.getenv("TARANTOOL_VERSION");
        return version == null ? "2.0" : version;
    }

}
