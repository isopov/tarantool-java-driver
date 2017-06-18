package com.sopovs.moradanen.tarantool;

public interface TupleWriter {

	public static TupleWriter EMPTY = tuple -> tuple.writeSize(0);

	void writeTuple(Tuple tuple);

	public static TupleWriter integer(int val) {
		return tuple -> {
			tuple.writeSize(1);
			tuple.writeInt(val);
		};
	}

	public static TupleWriter string(String val) {
		return tuple -> {
			tuple.writeSize(1);
			tuple.writeString(val);
		};
	}

}
