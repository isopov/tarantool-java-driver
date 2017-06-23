package com.sopovs.moradanen.tarantool;

import java.io.Closeable;

public interface TarantoolClientSource extends Closeable {

	TarantoolClient getClient();

	@Override
	void close();
}
