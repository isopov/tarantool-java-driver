package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.*;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

//TODO lazy clients creation and closing clients that are stale for too long
public class TarantoolPooledClientSource implements TarantoolClientSource {

    static final String CONNECTION_CLOSED = "Connection already closed";
    static final String POOL_CLOSED = "Pool is closed";

    private boolean poolClosed = false;
    private final ArrayDeque<IndexedTarantoolClient> pool;
    private final int size;
    private int currentSize;
    private final TarantoolConfig[] configs;
    private final int[] sizes;
    private final int[] currentSizes;
    private final Function<TarantoolConfig, TarantoolClient> clientFactory;

    public TarantoolPooledClientSource(Map<TarantoolConfig, Integer> configs) {
        this(configs, TarantoolClientImpl::new);
    }

    public TarantoolPooledClientSource(Map<TarantoolConfig, Integer> configSizes, Function<TarantoolConfig, TarantoolClient> clientFactory) {
        sizes = new int[configSizes.size()];

        currentSizes = new int[configSizes.size()];
        configs = new TarantoolConfig[configSizes.size()];
        int i = 0, totalSize = 0;

        for (Map.Entry<TarantoolConfig, Integer> configSize : configSizes.entrySet()) {
            sizes[i] = configSize.getValue();
            totalSize += sizes[i];
            configs[i] = configSize.getKey();
            i++;
        }

        this.clientFactory = clientFactory;
        this.size = totalSize;
        this.pool = new ArrayDeque<>(totalSize);

    }

    public TarantoolPooledClientSource(TarantoolConfig config, Function<TarantoolConfig, TarantoolClient> clientFactory,
                                       int size) {
        this.configs = new TarantoolConfig[]{config};
        this.clientFactory = clientFactory;
        this.size = size;
        this.sizes = new int[]{size};
        this.currentSizes = new int[]{0};
        this.pool = new ArrayDeque<>(size);
    }

    public TarantoolPooledClientSource(TarantoolConfig config, int size) {
        this(config, TarantoolClientImpl::new, size);
    }

    public TarantoolPooledClientSource(@Nullable String host, int port, int size) {
        this(host, port, null, null, size);
    }

    public TarantoolPooledClientSource(@Nullable String host, int port, @Nullable String login, @Nullable String password, int size) {
        this(new TarantoolConfig(host, port, login, password), TarantoolClientImpl::new, size);
    }


    @Override
    public TarantoolClient getClient() {
        synchronized (pool) {
            while (!poolClosed) {
                IndexedTarantoolClient client = pool.pollFirst();
                if (client != null) {
                    return new TarantoolClientProxy(client);
                }
                if (currentSize < size) {
                    for (int i = 0; i < configs.length; i++) {
                        if (currentSizes[i] < sizes[i]) {
                            try {
                                currentSize++;
                                currentSizes[i]++;
                                return new TarantoolClientProxy(new IndexedTarantoolClient(clientFactory.apply(configs[i]), i));
                            } catch (TarantoolException creationException) {
                                currentSize--;
                                currentSizes[i]--;
                                throw creationException;
                            }
                        }
                    }
                    throw new IllegalStateException("currentSize does not sum curentSizes");
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
            for (Iterator<IndexedTarantoolClient> iterator = pool.iterator(); iterator.hasNext(); ) {
                IndexedTarantoolClient client = iterator.next();
                try {
                    client.client.close();
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

    private static final class IndexedTarantoolClient {
        private final TarantoolClient client;
        private final int config;

        private IndexedTarantoolClient(TarantoolClient client, int config) {
            this.client = client;
            this.config = config;
        }
    }

    private final class TarantoolClientProxy implements TarantoolClient {
        private final IndexedTarantoolClient client;
        private boolean closed = false;
        @Nullable
        private Result lastResult;

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
                    client.client.close();
                } else {
                    if (lastResult != null && lastResult.hasNext()) {
                        lastResult.close();
                        lastResult = null;
                    }
                    pool.add(client);
                    pool.notify();
                }
            }
        }

        private TarantoolException closeOnException(TarantoolException e) {
            synchronized (pool) {
                closed = true;
                assert currentSize > 0;
                assert currentSizes[client.config] > 0;
                currentSize--;
                currentSizes[client.config]--;
                pool.notify();
            }
            try {
                client.client.close();
            } catch (TarantoolException closeException) {
                e.addSuppressed(closeException);
            }
            return e;
        }

        TarantoolClientProxy(IndexedTarantoolClient client) {
            this.client = client;
        }

        @Override
        public Result execute() {
            checkClosed();
            try {
                return lastResult = client.client.execute();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void addBatch() {
            checkClosed();
            try {
                client.client.addBatch();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public int[] executeBatchUpdate() {
            checkClosed();
            try {
                return client.client.executeBatchUpdate();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void executeBatch() {
            checkClosed();
            try {
                client.client.executeBatch();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void select(int space, int index, int limit, int offset, Iter iterator) {
            checkClosed();
            try {
                client.client.select(space, index, limit, offset, iterator);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void selectAll(int space, int limit, int offset) {
            checkClosed();
            try {
                client.client.selectAll(space, limit, offset);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void eval(String expression) {
            checkClosed();
            try {
                client.client.eval(expression);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void insert(int space) {
            checkClosed();
            try {
                client.client.insert(space);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void replace(int space) {
            checkClosed();
            try {
                client.client.replace(space);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void delete(int space, int index) {
            checkClosed();
            try {
                client.client.delete(space, index);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void update(int space, int index) {
            checkClosed();
            try {
                client.client.update(space, index);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void upsert(int space) {
            checkClosed();
            try {
                client.client.upsert(space);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void change(IntOp op, int field, int arg) {
            checkClosed();
            try {
                client.client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void change(IntOp op, int field, long arg) {
            checkClosed();
            try {
                client.client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }

        }

        @Override
        public void change(Op op, int field, @Nullable String arg) {
            checkClosed();
            try {
                client.client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }

        }

        @Override
        public void change(Op op, int field, byte[] arg) {
            checkClosed();
            try {
                client.client.change(op, field, arg);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void sql(String sqlQuery) {
            checkClosed();
            try {
                client.client.sql(sqlQuery);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public int executeUpdate() {
            checkClosed();
            try {
                return client.client.executeUpdate();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void ping() {
            checkClosed();
            try {
                client.client.ping();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setInt(int val) {
            checkClosed();
            try {
                client.client.setInt(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setString(@Nullable String val) {
            checkClosed();
            try {
                client.client.setString(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setNull() {
            checkClosed();
            try {
                client.client.setNull();
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setBoolean(boolean val) {
            checkClosed();
            try {
                client.client.setBoolean(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setDouble(double val) {
            try {
                client.client.setDouble(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setFloat(float val) {
            checkClosed();
            try {
                client.client.setFloat(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setLong(long val) {
            checkClosed();
            try {
                client.client.setLong(val);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public void setBytes(byte[] bytes) {
            checkClosed();
            try {
                client.client.setBytes(bytes);
            } catch (TarantoolException e) {
                throw closeOnException(e);
            }
        }

        @Override
        public String getVersion() {
            checkClosed();
            return client.client.getVersion();
        }

        @Override
        public void setNetworkTimeout(int milliseconds) {
            checkClosed();
            client.client.setNetworkTimeout(0);
        }

        @Override
        public int getNetworkTimeout() {
            checkClosed();
            return client.client.getNetworkTimeout();
        }

        private void checkClosed() {
            if (closed) {
                throw new TarantoolException(CONNECTION_CLOSED);
            }
        }

    }
}
