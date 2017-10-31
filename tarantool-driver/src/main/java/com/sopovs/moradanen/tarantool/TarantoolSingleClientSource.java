package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.Iter;
import com.sopovs.moradanen.tarantool.core.Op;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

public class TarantoolSingleClientSource implements TarantoolClientSource {
	private boolean closed = false;
	private boolean given = false;
	private final Object lock = new Object();
	private final TarantoolClientProxy clientProxy;

	public TarantoolSingleClientSource(TarantoolClient client) {
		clientProxy = new TarantoolClientProxy(client);
	}

	public TarantoolSingleClientSource(String host, int port) {
		clientProxy = new TarantoolClientProxy(new TarantoolClientImpl(host, port));
	}

	@Override
	public TarantoolClient getClient() {
		synchronized (lock) {
			while (!closed) {
				if (!given) {
					given = true;
					return clientProxy;
				}
				try {
					lock.wait();
				} catch (InterruptedException e) {
					throw new TarantoolException("Interrupted while waiting for a free connection", e);
				}
			}
		}
		throw new TarantoolException("Source is closed");
	}

	@Override
	public void close() {
		synchronized (lock) {
			closed = true;
			clientProxy.client.close();
		}

	}

	private final class TarantoolClientProxy implements TarantoolClient {
		private final TarantoolClient client;

		@Override
		public boolean isClosed() {
			// TODO maybe have separate proxies and not the shared one?
			return closed && !given;
		}

		@Override
		public void close() {
			synchronized (lock) {
				given = false;
				lock.notify();
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
		public int[] executeBatchUpdate() {
			return client.executeBatchUpdate();
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
		public void sql(String sqlQuery) {
			client.sql(sqlQuery);
		}

		@Override
		public int executeUpdate() {
			return client.executeUpdate();
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

		@Override
		public void setBytes(byte[] bytes) {
			client.setBytes(bytes);
		}

		@Override
		public String getVersion() {
			return client.getVersion();
		}
	}
}
