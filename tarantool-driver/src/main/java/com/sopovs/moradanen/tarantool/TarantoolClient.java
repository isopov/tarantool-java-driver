package com.sopovs.moradanen.tarantool;

public interface TarantoolClient {

	Result selectAll(int space, int limit, int offset);

	default Result selectAll(int space) {
		return selectAll(space, Integer.MAX_VALUE);
	}

	default Result selectAll(int space, int limit) {
		return selectAll(space, limit, 0);
	}

	default int space(String space) {
		try (Result result = select(Util.SPACE_VSPACE, space, Util.INDEX_SPACE_NAME)) {
			if (result.getSize() == 0) {
				throw new TarantoolException("No such space " + space);
			}
			if (result.getSize() != 1) {
				throw new TarantoolException("Unexpected result length " + result.getSize());
			}
			result.next();
			return result.getInt();
		}
	}

	default Result select(int space, String key, int index) {
		return select(space, key, index, Integer.MAX_VALUE, 0);
	}

	default Result eval(String expression) {
		return eval(expression, TupleWriter.EMPTY);
	}

	Result eval(String expression, TupleWriter tupleWriter);

	Result select(int space, String key, int index, int limit, int offset);

	Result select(int space, int key, int index, int limit, int offset);

}
