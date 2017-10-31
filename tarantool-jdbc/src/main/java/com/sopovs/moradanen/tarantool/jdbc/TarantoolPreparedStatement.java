package com.sopovs.moradanen.tarantool.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.sopovs.moradanen.tarantool.SqlResult;
import com.sopovs.moradanen.tarantool.TarantoolClient;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

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
		return new TarantoolResultSet(this, (SqlResult) client.execute());
	}

	private void executeAndSetParameters() throws SQLException {
		checkClosed();
		client.sql(sql);
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

	private void ensureParametersSize(int size) throws SQLException {
		checkClosed();
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
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, Types.OTHER);
		} else if (x instanceof String) {
			setString(parameterIndex, (String) x);
		} else if (x instanceof BigDecimal) {
			setBigDecimal(parameterIndex, (BigDecimal) x);
		} else if (x instanceof Short) {
			setShort(parameterIndex, (Short) x);
		} else if (x instanceof Integer) {
			setInt(parameterIndex, (Integer) x);
		} else if (x instanceof Long) {
			setLong(parameterIndex, (Long) x);
		} else if (x instanceof Float) {
			setFloat(parameterIndex, (Float) x);
		} else if (x instanceof Double) {
			setDouble(parameterIndex, (Double) x);
		} else if (x instanceof byte[]) {
			setBytes(parameterIndex, (byte[]) x);
		} else if (x instanceof java.sql.Date) {
			setDate(parameterIndex, (java.sql.Date) x);
		} else if (x instanceof Time) {
			setTime(parameterIndex, (Time) x);
		} else if (x instanceof Timestamp) {
			setTimestamp(parameterIndex, (Timestamp) x);
		} else if (x instanceof Boolean) {
			setBoolean(parameterIndex, (Boolean) x);
		} else if (x instanceof Byte) {
			setByte(parameterIndex, (Byte) x);
		} else if (x instanceof Blob) {
			setBlob(parameterIndex, (Blob) x);
		} else if (x instanceof Clob) {
			setClob(parameterIndex, (Clob) x);
		} else if (x instanceof Array) {
			setArray(parameterIndex, (Array) x);
		} else if (x instanceof Character) {
			setString(parameterIndex, ((Character) x).toString());
		} else if (x instanceof SQLXML) {
			setSQLXML(parameterIndex, (SQLXML) x);
		} else {
			throw new SQLException("Can''t infer the SQL type to use for an instance of" + x
					+ ". Use setObject() with an explicit Types value to specify the type to use.");
		}
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		parameters.set(parameterIndex - 1, client -> client.setBytes(x));
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		// TODO
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		// TODO
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		// TODO
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		if (x == null) {
			setNull(parameterIndex, Types.VARBINARY);
			return;
		}
		if (length < 0) {
			throw new SQLException("Invalid stream length " + length);
		}
		parameters.set(parameterIndex - 1, client -> client.setBytes(toByteArray(x)));
	}

	private static byte[] toByteArray(InputStream in) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, in.available()));
			copy(in, out);
			return out.toByteArray();
		} catch (IOException e) {
			throw new TarantoolException(e);
		}
	}

	private static long copy(InputStream from, OutputStream to) throws IOException {
		byte[] buf = new byte[8192];
		long total = 0;
		while (true) {
			int r = from.read(buf);
			if (r == -1) {
				break;
			}
			to.write(buf, 0, r);
			total += r;
		}
		return total;
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
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
		ensureParametersSize(parameterIndex - 1);
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		ensureParametersSize(parameterIndex - 1);
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
