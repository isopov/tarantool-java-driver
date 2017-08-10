package com.sopovs.moradanen.tarantool.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.sopovs.moradanen.tarantool.MapResult;
import com.sopovs.moradanen.tarantool.TarantoolClient;

public class TarantoolPreparedStatement extends TarantoolStatement implements PreparedStatement {

	private final String sql;
	private final List<Parameter> parameters = new ArrayList<>();

	public TarantoolPreparedStatement(TarantoolConnection connection, String sql) throws SQLException {
		super(connection);
		this.sql = sql;
	}

	@Override
	public TarantoolResultSet executeQuery() throws SQLException {
		executeAndSetParameters();
		return new TarantoolResultSet(this, (MapResult) client.execute());
	}

	private void executeAndSetParameters() throws SQLException {
		client.execute(sql);
		for (int i = 0; i < parameters.size(); i++) {
			requireParameter(parameters.get(i), i).set(client);
		}
	}

	private static Parameter requireParameter(Parameter par, int index) throws SQLException {
		if (par == null) {
			throw new SQLException("Parameter " + (index + 1) + " is not set");
		}
		return par;
	}

	@Override
	public int executeUpdate() throws SQLException {
		executeAndSetParameters();
		return (int) client.executeUpdate();
	}

	private void ensureParametersSize(int size) {
		while (parameters.size() <= size) {
			parameters.add(null);
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, TarantoolClient::setNull);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setBoolean(x));
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setInt(x));

	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setInt(x));

	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setInt(x));

	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setLong(x));
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setFloat(x));
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setDouble(x));

	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		if (x == null) {
			parameters.set(parameterIndex - 1, TarantoolClient::setNull);
		} else {
			parameters.set(parameterIndex - 1, client -> client.setString(x));
		}
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, TarantoolClient::setNull);
	}

	@Override
	public void clearParameters() throws SQLException {
		parameters.clear();
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch() throws SQLException {
		executeAndSetParameters();
		client.addBatch();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	private interface Parameter {
		void set(TarantoolClient client);
	}

}
