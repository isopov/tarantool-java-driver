package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.IntOp;
import com.sopovs.moradanen.tarantool.core.Iter;
import com.sopovs.moradanen.tarantool.core.Op;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.Function;

//TODO lazy clients creation and closing clients that are stale for too long
public class TarantoolPooledClientSource implements TarantoolClientSource {

    static final String CONNECTION_CLOSED = "Connection already closed";
    static final String POOL_CLOSED = "Pool is closed";

    private boolean poolClosed = false;
    private final ArrayDeque<TarantoolClient> pool;
    private final int size;
    private int currentSize;
    private final TarantoolConfig config;
    private final Function<TarantoolConfig, TarantoolClient> clientFactory;

    public TarantoolPooledClientSource(TarantoolConfig config, Function<TarantoolConfig, TarantoolClient> clientFactory,
                                       int size) {
        this.config = config;
        this.clientFactory = clientFactory;
        this.size = size;
        this.pool = new ArrayDeque<>(size);
    }

    public TarantoolPooledClientSource(TarantoolConfig config, int size) {
        this(config, TarantoolClientImpl::new, size);
    }

    public TarantoolPooledClientSource(String host, int port, int size) {
        this(new TarantoolConfig(host, port, null, null), TarantoolClientImpl::new, size);
    }

    @Override
    public TarantoolClient getClient() {
        synchronized (pool) {
            while (!poolClosed) {
                TarantoolClient client = pool.pollFirst();
                if (client != null) {
                    return new TarantoolClientProxy(client);
                }
                if (currentSize < size) {
                    currentSize++;
                    return new TarantoolClientProxy(clientFactory.apply(config));
                }
                try {
                    pool.wait();
                } catch (InterruptedException e) {
                    throw new TarantoolException("Interrupted while waiting for a free connection");
                }
            }
        }
        throw new TarantoolException(POOL_CLOSED);
    }

    @Override
    public void close() {
        synchronized (pool) {
            poolClosed = true;
            TarantoolException poolCloseException = null;
            for (Iterator<TarantoolClient> iterator = pool.iterator(); iterator.hasNext(); ) {
                TarantoolClient client = iterator.next();
                try {
                    client.close();
                } catch (TarantoolException e) {
                    if (poolCloseException == null) {
                        poolCloseException = new TarantoolException("Problem closing pooled client(s)");
                    }
                    poolCloseException.addSuppressed(e);
                }
                iterator.remove();
            }
            pool.notifyAll();
            if (poolCloseException != null) {
                throw poolCloseException;
            }
        }

    }

    private final class TarantoolClientProxy implements TarantoolClient {
        private final TarantoolClient client;
        private boolean closed = false;

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            synchronized (pool) {
                if (closed) {
                    return;
                }
                closed = true;
                if (poolClosed) {
                    client.close();
                } else {
                    pool.add(client);
                    pool.notify();
                }
            }
        }

        private TarantoolException closeOnException(TarantoolException e) {
            synchronized (pool) {
                closed = true;
                assert currentSize > 0;
                currentSize--;
                pool.notify();
            }
            try {
                client.close();
            } catch (TarantoolException closeException) {
                e.addSuppressed(closeException);
            }
            return e;
        }

        public TarantoolClientProxy(TarantoolClient client) {
            this.client = client;
        }

        @Override
        public Result execute() {
            checkClosed();
            try {
                return client.execute();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void addBatch() {
            checkClosed();
            try {
                client.addBatch();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public int[] executeBatchUpdate() {
            checkClosed();
            try {
                return client.executeBatchUpdate();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void executeBatch() {
            checkClosed();
            try {
                client.executeBatch();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void select(int space, int index, int limit, int offset, Iter iterator) {
            checkClosed();
            try {
                client.select(space, index, limit, offset, iterator);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void selectAll(int space, int limit, int offset) {
            checkClosed();
            try {
                client.selectAll(space, limit, offset);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void eval(String expression) {
            checkClosed();
            try {
                client.eval(expression);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void insert(int space) {
            checkClosed();
            try {
                client.insert(space);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void replace(int space) {
            checkClosed();
            try {
                client.replace(space);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void delete(int space, int index) {
            checkClosed();
            try {
                client.delete(space, index);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void update(int space, int index) {
            checkClosed();
            try {
                client.update(space, index);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void upsert(int space) {
            checkClosed();
            try {
                client.upsert(space);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void change(IntOp op, int field, int arg) {
            checkClosed();
            try {
                client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void change(IntOp op, int field, long arg) {
            checkClosed();
            try {
                client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }

        }

        @Override
        public void change(Op op, int field, String arg) {
            checkClosed();
            try {
                client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }

        }

        @Override
        public void change(Op op, int field, byte[] arg) {
            checkClosed();
            try {
                client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void sql(String sqlQuery) {
            checkClosed();
            try {
                client.sql(sqlQuery);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public int executeUpdate() {
            checkClosed();
            try {
                return client.executeUpdate();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void ping() {
            checkClosed();
            try {
                client.ping();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setInt(int val) {
            checkClosed();
            try {
                client.setInt(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setString(String val) {
            checkClosed();
            try {
                client.setString(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setNull() {
            checkClosed();
            try {
                client.setNull();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setBoolean(boolean val) {
            checkClosed();
            try {
                client.setBoolean(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setDouble(double val) {
            try {
                client.setDouble(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setFloat(float val) {
            checkClosed();
            try {
                client.setFloat(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setLong(long val) {
            checkClosed();
            try {
                client.setLong(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setBytes(byte[] bytes) {
            checkClosed();
            try {
                client.setBytes(bytes);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public String getVersion() {
            checkClosed();
            return client.getVersion();
        }

        private void checkClosed() {
            if (closed) {
                throw new TarantoolException(CONNECTION_CLOSED);
            }
        }

    }
}
