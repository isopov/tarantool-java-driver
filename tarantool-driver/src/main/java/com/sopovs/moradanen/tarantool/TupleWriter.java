package com.sopovs.moradanen.tarantool;

public interface TupleWriter {

	public static TupleWriter EMPTY = tuple -> tuple.writeSize(0);

	void writeTuple(Tuple tuple);

}
