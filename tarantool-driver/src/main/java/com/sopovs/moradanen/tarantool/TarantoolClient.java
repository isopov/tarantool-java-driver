package com.sopovs.moradanen.tarantool;

public interface TarantoolClient {

	default Result selectAll(int space, int limit, int offset) {
		return select(space, TupleWriter.EMPTY, 0, limit, offset);
	}

	default Result selectAll(String space, int limit, int offset) {
		return selectAll(space(space), limit, offset);
	}

	default Result selectAll(int space) {
		return selectAll(space, Integer.MAX_VALUE);
	}

	default Result selectAll(String space) {
		return selectAll(space(space), Integer.MAX_VALUE);
	}

	default Result selectAll(int space, int limit) {
		return selectAll(space, limit, 0);
	}

	default Result selectAll(String space, int limit) {
		return selectAll(space(space), limit, 0);
	}

	default int space(String space) {
		Result result = select(Util.SPACE_VSPACE, space, Util.INDEX_SPACE_NAME);
		if (result.getSize() == 0) {
			throw new TarantoolException("No such space " + space);
		}
		if (result.getSize() != 1) {
			throw new TarantoolException("Unexpected result length " + result.getSize());
		}
		result.next();
		return result.getInt(0);
	}

	Result select(int space, TupleWriter keyWriter, int index, int limit, int offset);

	default Result select(int space, String key, int index, int limit, int offset) {
		return select(space, TupleWriter.string(key), index, limit, offset);
	}

	default Result select(String space, String key, int index, int limit, int offset) {
		return select(space(space), key, index, limit, offset);
	}

	default Result select(int space, String key, int index) {
		return select(space, key, index, Integer.MAX_VALUE, 0);
	}

	default Result select(String space, String key, int index) {
		return select(space(space), key, index, Integer.MAX_VALUE, 0);
	}

	default Result select(int space, String key, int index, int limit) {
		return select(space, key, index, limit, 0);
	}

	default Result select(String space, String key, int index, int limit) {
		return select(space(space), key, index, limit, 0);
	}

	default Result select(int space, int key, int index, int limit, int offset) {
		return select(space, TupleWriter.integer(key), index, limit, offset);
	}

	default Result select(int space, int key, int index) {
		return select(space, key, index, Integer.MAX_VALUE, 0);
	}

	default Result select(String space, int key, int index) {
		return select(space(space), key, index, Integer.MAX_VALUE, 0);
	}

	default Result select(int space, int key, int index, int limit) {
		return select(space, key, index, limit, 0);
	}

	default Result select(String space, int key, int index, int limit) {
		return select(space(space), key, index, limit, 0);
	}

	Result eval(String expression, TupleWriter tupleWriter);

	default Result eval(String expression) {
		return eval(expression, TupleWriter.EMPTY);
	}

	Result insert(int space, TupleWriter tupleWriter);

	default Result insert(String space, TupleWriter tupleWriter) {
		return insert(space(space), tupleWriter);
	}

	Result replace(int space, TupleWriter tupleWriter);

	default Result replace(String space, TupleWriter tupleWriter) {
		return replace(space(space), tupleWriter);
	}

	Result delete(int space, TupleWriter keyWriter, int index);

	default Result delete(String space, TupleWriter keyWriter, int index) {
		return delete(space(space), keyWriter, index);
	}

	default Result delete(String space, int key, int index) {
		return delete(space(space), TupleWriter.integer(key), index);
	}

	default Result delete(String space, int key) {
		return delete(space(space), TupleWriter.integer(key), 0);
	}

	default Result delete(String space, TupleWriter keyWriter) {
		return delete(space(space), keyWriter, 0);
	}

	void ping();
}
