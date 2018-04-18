package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.*;

import java.io.Closeable;

public interface TarantoolClient extends Closeable {

    Result execute();

    void addBatch();

    void executeBatch();

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

    void select(int space, int index, int limit, int offset, Iter iterator);

    default void select(int space, int index, int limit, int offset) {
        select(space, index, limit, offset, Iter.EQ);
    }

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

    void selectAll(int space, int limit, int offset);

    default void selectAll(int space) {
        selectAll(space, Integer.MAX_VALUE, 0);
    }

    default void selectAll(String space) {
        selectAll(space(space), Integer.MAX_VALUE, 0);
    }

    default void selectAll(int space, int limit) {
        selectAll(space, limit, 0);
    }

    default void selectAll(String space, int limit) {
        selectAll(space(space), limit, 0);
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

    void upsert(int space);

    void change(IntOp op, int field, int arg);

    void change(IntOp op, int field, long arg);

    void change(Op op, int field, String arg);

    void change(Op op, int field, byte[] arg);

    void ping();

    void setNull();

    void setBoolean(boolean val);

    void setDouble(double val);

    void setFloat(float val);

    void setInt(int val);

    void setLong(long val);

    void setString(String val);

    void setBytes(byte[] bytes);

    String getVersion();

    void sql(String sqlQuery);

    boolean isClosed();

    @Override
    void close();

    int executeUpdate();

    int[] executeBatchUpdate();

}
