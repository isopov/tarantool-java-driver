package com.sopovs.moradanen.tarantool;

public interface TarantoolClient {

	Result execute();

	void addBatch();

	void executeBatch();

	default void selectAll(int space, int limit, int offset) {
		select(space, TupleWriter.EMPTY, 0, limit, offset);
	}

	default void selectAll(int space, int limit) {
		selectAll(space, limit, 0);
	}

	default void selectAll(String space, int limit) {
		selectAll(space(space), limit, 0);
	}

	default int space(String space) {
		select(Util.SPACE_VSPACE, space, Util.INDEX_SPACE_NAME);
		Result result = execute();
		if (result.getSize() == 0) {
			throw new TarantoolException("No such space " + space);
		}
		if (result.getSize() != 1) {
			throw new TarantoolException("Unexpected result length " + result.getSize());
		}
		result.next();
		return result.getInt(0);
	}

	void select(int space, TupleWriter keyWriter, int index, int limit, int offset);

	default void select(int space, String key, int index, int limit, int offset) {
		select(space, TupleWriter.string(key), index, limit, offset);
	}

	default void select(String space, String key, int index, int limit, int offset) {
		select(space(space), key, index, limit, offset);
	}

	default void select(int space, String key, int index) {
		select(space, key, index, Integer.MAX_VALUE, 0);
	}

	default void select(int space, int key, int index, int limit, int offset) {
		select(space, TupleWriter.integer(key), index, limit, offset);
	}

	default void select(String space, int key, int index) {
		select(space(space), key, index, Integer.MAX_VALUE, 0);
	}

	default void select(int space, int key, int index, int limit) {
		select(space, key, index, limit, 0);
	}

	default void select(String space, int key, int index, int limit) {
		select(space(space), key, index, limit, 0);
	}

	void eval(String expression, TupleWriter tupleWriter);

	default void eval(String expression) {
		eval(expression, TupleWriter.EMPTY);
	}

	default Result evalFully(String expression) {
		eval(expression);
		return execute();
	}

	void insert(int space, TupleWriter tupleWriter);

	default void insert(String space, TupleWriter tupleWriter) {
		insert(space(space), tupleWriter);
	}

	void replace(int space, TupleWriter tupleWriter);

	default void replace(String space, TupleWriter tupleWriter) {
		replace(space(space), tupleWriter);
	}

	void delete(int space, TupleWriter keyWriter, int index);

	default void delete(String space, TupleWriter keyWriter, int index) {
		delete(space(space), keyWriter, index);
	}

	default void delete(String space, int key, int index) {
		delete(space(space), TupleWriter.integer(key), index);
	}

	default void delete(String space, int key) {
		delete(space(space), TupleWriter.integer(key), 0);
	}

	default void delete(String space, TupleWriter keyWriter) {
		delete(space(space), keyWriter, 0);
	}

	void ping();
}
