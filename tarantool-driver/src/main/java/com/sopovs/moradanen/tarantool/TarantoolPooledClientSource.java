package com.sopovs.moradanen.tarantool;

import java.util.ArrayDeque;
import java.util.Iterator;

//TODO remove clients after exceptions from pool
//TODO add clients to the size after removing
//TODO lazy clients creation and closing clients that are stale for too long
public class TarantoolPooledClientSource implements TarantoolClientSource {

	private boolean closed = false;
	private final ArrayDeque<TarantoolClientProxy> pool;

	public TarantoolPooledClientSource(String host, int port, int size) {
		pool = new ArrayDeque<>(size);
		for (int i = 0; i < size; i++) {
			pool.add(new TarantoolClientProxy(new TarantoolClientImpl(host, port)));
		}
	}

	@Override
	public TarantoolClient getClient() {
		synchronized (pool) {
			while (!closed) {
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
			closed = true;
			for (Iterator<TarantoolClientProxy> iterator = pool.iterator(); iterator.hasNext();) {
				TarantoolClientProxy proxy = iterator.next();
				// TODO handle close exceptions
				proxy.client.close();
				iterator.remove();
			}
		}

	}

	private final class TarantoolClientProxy implements TarantoolClient {
		private final TarantoolClient client;

		@Override
		public void close() {
			synchronized (pool) {
				if (closed) {
					client.close();
				} else {
					pool.add(this);
					pool.notify();
				}
			}
		}

		public TarantoolClientProxy(TarantoolClient client) {
			this.client = client;
		}

		@Override
		public Result execute() {
			return client.execute();
		}

		@Override
		public void addBatch() {
			client.addBatch();

		}

		@Override
		public void executeBatch() {
			client.executeBatch();
		}

		@Override
		public void select(int space, int index, int limit, int offset, Iter iterator) {
			client.select(space, index, limit, offset, iterator);
		}

		@Override
		public void selectAll(int space, int limit, int offset) {
			client.selectAll(space, limit, offset);

		}

		@Override
		public void eval(String expression) {
			client.eval(expression);
		}

		@Override
		public void insert(int space) {
			client.insert(space);
		}

		@Override
		public void replace(int space) {
			client.replace(space);
		}

		@Override
		public void delete(int space, int index) {
			client.delete(space, index);

		}

		@Override
		public void update(int space, int index) {
			client.update(space, index);
		}

		@Override
		public void upsert(int space) {
			client.upsert(space);

		}

		@Override
		public void change(Op op, int field, int arg) {
			client.change(op, field, arg);

		}

		@Override
		public void ping() {
			client.ping();

		}

		@Override
		public void setInt(int val) {
			client.setInt(val);
		}

		@Override
		public void setString(String val) {
			client.setString(val);
		}

		@Override
		public void setNull() {
			client.setNull();
		}

		@Override
		public void setBoolean(boolean val) {
			client.setBoolean(val);
		}

		@Override
		public void setDouble(double val) {
			client.setDouble(val);
		}

		@Override
		public void setFloat(float val) {
			client.setFloat(val);
		}

		@Override
		public void setLong(long val) {
			client.setLong(val);
		}
	}
}
