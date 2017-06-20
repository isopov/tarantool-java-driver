package com.sopovs.moradanen.tarantool;

public interface TarantoolClient {

	Result execute();

	void addBatch();

	void executeBatch();

	default void selectAll(int space, int limit, int offset) {
		select(space, 0, limit, offset);
	}

	default void selectAll(int space, int limit) {
		selectAll(space, limit, 0);
	}

	default void selectAll(String space, int limit) {
		selectAll(space(space), limit, 0);
	}

	default int space(String space) {
		select(Util.SPACE_VSPACE, Util.INDEX_SPACE_NAME);
		setString(space);
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

	void select(int space, int index, int limit, int offset);

	default void select(String space, int index, int limit, int offset) {
		select(space(space), index, limit, offset);
	}

	default void select(int space, int index) {
		select(space, index, Integer.MAX_VALUE, 0);
	}

	default void select(String space, int index) {
		select(space(space), index, Integer.MAX_VALUE, 0);
	}

	default void select(int space, int index, int limit) {
		select(space, index, limit, 0);
	}

	default void select(String space, int index, int limit) {
		select(space(space), index, limit, 0);
	}

	void eval(String expression);

	default Result evalFully(String expression) {
		eval(expression);
		return execute();
	}

	void insert(int space);

	default void insert(String space) {
		insert(space(space));
	}

	void replace(int space);

	default void replace(String space) {
		replace(space(space));
	}

	void delete(int space, int index);

	default void delete(String space, int index) {
		delete(space(space), index);
	}

	default void delete(String space) {
		delete(space(space), 0);
	}

	void update(int space, int index);

	void change(Op op, int field, int arg);

	void ping();

	void setInt(int val);

	void setString(String val);

	enum Op {
		// TODO Delete
		PLUS("+"), MINUS("-"), AND("&"), OR("|"), XOR("^");

		private final String val;

		private Op(String val) {
			this.val = val;
		}

		public String getVal() {
			return val;
		}
	}

}
