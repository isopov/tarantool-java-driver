package com.sopovs.moradanen.tarantool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.sopovs.moradanen.tarantool.core.Iter;
import com.sopovs.moradanen.tarantool.core.Op;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

public class TarantoolPooledClientSourceTest {
	private static final TarantoolConfig DUMMY_CONFIG = new TarantoolConfig(null, 0, null, null);
	private static final int POOL_SIZE = 5;

	@Test
	public void testSimple() {
		try (TarantoolClientSource pool = createPool()) {
		}
	}

	private TarantoolPooledClientSource createPool() {
		return createPool(POOL_SIZE);
	}

	private TarantoolPooledClientSource createPool(int size) {
		return new TarantoolPooledClientSource(DUMMY_CONFIG, DummyTarantoolClient::new, size);
	}

	@Test
	public void releaseWaiterOnPoolClose() throws Exception {
		ExecutorService threadPool = Executors.newFixedThreadPool(POOL_SIZE);
		List<Future<?>> futures = new ArrayList<>();
		try (TarantoolClientSource pool = createPool(0)) {
			for (int i = 0; i < POOL_SIZE; i++) {
				futures.add(threadPool.submit(() -> {
					try (TarantoolClient waitingClient = pool.getClient()) {
					}
				}));
			}
		}
		assertEquals(POOL_SIZE, futures.size());
		threadPool.shutdown();
		threadPool.awaitTermination(1, TimeUnit.SECONDS);
		for (Future<?> future : futures) {
			assertTrue(future.isDone());
			try {
				future.get();
				fail("Exception expected");
			} catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof TarantoolException);
				assertEquals("Pool is closed", e.getCause().getMessage());
			}
		}
	}

	@Test
	public void testGet1Connection() {
		try (TarantoolClientSource pool = createPool(); TarantoolClient client = pool.getClient()) {
		}
	}

	@Test
	public void testGetAllConnections() throws Exception {
		try (TarantoolClientSource pool = createPool()) {
			testGetAllConnectionsInternal(pool);
		}
	}

	@Test
	public void testGetAllConnectionsAfterException() throws Exception {
		try (TarantoolClientSource pool = createPool()) {
			try (TarantoolClient client = pool.getClient()) {
				client.ping();
				fail("Exception expected");
			} catch (Exception e) {
				// no code
			}
			testGetAllConnectionsInternal(pool);
		}
	}

	private void testGetAllConnectionsInternal(TarantoolClientSource pool) throws InterruptedException {
		ExecutorService threadPool = Executors.newFixedThreadPool(POOL_SIZE);
		CountDownLatch latch = new CountDownLatch(POOL_SIZE);
		for (int i = 0; i < POOL_SIZE; i++) {
			threadPool.execute(() -> {
				try (TarantoolClient client = pool.getClient()) {
					latch.countDown();
					try {
						latch.await();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}
		threadPool.shutdown();
		threadPool.awaitTermination(1, TimeUnit.SECONDS);
	}

	public static class DummyTarantoolClient implements TarantoolClient {

		public DummyTarantoolClient(TarantoolConfig config) {
			// dummy
		}

		@Override
		public boolean isClosed() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void close() {
			// dummy
		}

		@Override
		public Result execute() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void addBatch() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public int[] executeBatchUpdate() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void executeBatch() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void select(int space, int index, int limit, int offset, Iter iterator) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void selectAll(int space, int limit, int offset) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void eval(String expression) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void insert(int space) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void replace(int space) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void delete(int space, int index) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void update(int space, int index) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void upsert(int space) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void change(Op op, int field, int arg) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void sql(String sqlQuery) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public int executeUpdate() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void ping() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setNull() {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setBoolean(boolean val) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setDouble(double val) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setFloat(float val) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setInt(int val) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setLong(long val) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setString(String val) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public void setBytes(byte[] bytes) {
			throw new TarantoolException("Not implemented!");
		}

		@Override
		public String getVersion() {
			throw new TarantoolException("Not implemented!");
		}
	}
}
