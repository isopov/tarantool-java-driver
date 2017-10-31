package com.sopovs.moradanen.tarantool;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.Function;

import com.sopovs.moradanen.tarantool.core.Iter;
import com.sopovs.moradanen.tarantool.core.Op;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

//TODO lazy clients creation and closing clients that are stale for too long
public class TarantoolPooledClientSource implements TarantoolClientSource {

	private boolean poolClosed = false;
	private final ArrayDeque<TarantoolClientProxy> pool;
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

	public TarantoolPooledClientSource(String host, int port, int size) {
		this(new TarantoolConfig(host, port, null, null), TarantoolClientImpl::new, size);
	}

	@Override
	public TarantoolClient getClient() {
		synchronized (pool) {
			while (!poolClosed) {
				if (currentSize < size) {
					return new TarantoolClientProxy(clientFactory.apply(config));
				}
				TarantoolClientProxy client = pool.pollFirst();
				if (client != null) {
					return client;
				}
				try {
					pool.wait();
				} catch (InterruptedException e) {
					throw new TarantoolException("Interrupted while waiting for a free connection");
				}
			}
		}
		throw new TarantoolException("Pool is closed");
	}

	@Override
	public void close() {
		synchronized (pool) {
			poolClosed = true;
			TarantoolException poolCloseException = null;
			for (Iterator<TarantoolClientProxy> iterator = pool.iterator(); iterator.hasNext();) {
				TarantoolClientProxy proxy = iterator.next();
				try {
					proxy.client.close();
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
				if (poolClosed) {
					closed = true;
					client.close();
				} else {
					pool.add(this);
					pool.notify();
				}
			}
		}

		private TarantoolException closeOnException(TarantoolException e) {
			synchronized (pool) {
				closed = true;
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
			try {
				return client.execute();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void addBatch() {
			try {
				client.addBatch();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public int[] executeBatchUpdate() {
			try {
				return client.executeBatchUpdate();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void executeBatch() {
			try {
				client.executeBatch();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void select(int space, int index, int limit, int offset, Iter iterator) {
			try {
				client.select(space, index, limit, offset, iterator);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void selectAll(int space, int limit, int offset) {
			try {
				client.selectAll(space, limit, offset);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void eval(String expression) {
			try {
				client.eval(expression);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void insert(int space) {
			try {
				client.insert(space);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void replace(int space) {
			try {
				client.replace(space);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void delete(int space, int index) {
			try {
				client.delete(space, index);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void update(int space, int index) {
			try {
				client.update(space, index);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void upsert(int space) {
			try {
				client.upsert(space);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void change(Op op, int field, int arg) {
			try {
				client.change(op, field, arg);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void sql(String sqlQuery) {
			try {
				client.sql(sqlQuery);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public int executeUpdate() {
			try {
				return client.executeUpdate();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void ping() {
			try {
				client.ping();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void setInt(int val) {
			try {
				client.setInt(val);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void setString(String val) {
			try {
				client.setString(val);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void setNull() {
			try {
				client.setNull();
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void setBoolean(boolean val) {
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
			try {
				client.setFloat(val);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void setLong(long val) {
			try {
				client.setLong(val);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public void setBytes(byte[] bytes) {
			try {
				client.setBytes(bytes);
			} catch (TarantoolException e) {
				throw closeOnException(e);
			}
		}

		@Override
		public String getVersion() {
			return client.getVersion();
		}
	}
}
