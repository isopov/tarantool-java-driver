package com.sopovs.moradanen.tarantool.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

public class TarantoolConnection implements Connection {

	private final TarantoolClient client;

	public TarantoolConnection(TarantoolClient client) {
		this.client = client;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (isWrapperFor(iface)) {
			return iface.cast(client);
		}
		throw new SQLException("not a wrapper for " + iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(client.getClass());
	}

	@Override
	public TarantoolStatement createStatement() throws SQLException {
		return new TarantoolStatement(this);
	}

	@Override
	public TarantoolPreparedStatement prepareStatement(String sql) throws SQLException {
		return new TarantoolPreparedStatement(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return sql;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (autoCommit) {
			throw new SQLFeatureNotSupportedException();
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return true;
	}

	@Override
	public void commit() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void close() throws SQLException {
		try {
			client.close();
		} catch (TarantoolException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return client.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public String getCatalog() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return Collections.emptyMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		if (timeout != 0) {
			throw new SQLFeatureNotSupportedException();
		}
		try {
			client.ping();
			return true;
		} catch (TarantoolException e) {
			return false;
		}
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new SQLClientInfoException();

	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLClientInfoException();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public String getSchema() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
