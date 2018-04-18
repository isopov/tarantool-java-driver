package com.sopovs.moradanen.tarantool.jdbc;

import com.sopovs.moradanen.tarantool.SqlResult;
import com.sopovs.moradanen.tarantool.core.TarantoolException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class TarantoolResultSet implements ResultSet {
    private final TarantoolStatement statement;
    private final SqlResult result;
    private boolean wasNull = false;

    TarantoolResultSet(TarantoolStatement statement, SqlResult result) {
        this.statement = statement;
        this.result = result;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(result);
        }
        throw new SQLException("not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return SqlResult.class.equals(iface);
    }

    @Override
    public boolean next() {
        return result.next();
    }

    @Override
    public void close()  {
        result.consume();
    }

    @Override
    public boolean wasNull()  {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex)  {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        return result.getString(columnIndex - 1);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return false;
        }
        wasNull = false;
        return result.getBoolean(columnIndex - 1);
    }

    @Override
    public byte getByte(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return (byte) result.getInt(columnIndex - 1);
    }

    @Override
    public short getShort(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return (short) result.getInt(columnIndex - 1);
    }

    @Override
    public int getInt(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return result.getInt(columnIndex - 1);
    }

    @Override
    public long getLong(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return result.getLong(columnIndex - 1);
    }

    @Override
    public float getFloat(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return result.getFloat(columnIndex - 1);
    }

    @Override
    public double getDouble(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return result.getDouble(columnIndex - 1);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return result.getBytes(columnIndex - 1);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)  {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        return new ByteBufferBackedInputStream(result.getByteBuffer(columnIndex - 1));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            return getString(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        try {
            return getBoolean(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        try {
            return getByte(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        try {
            return getShort(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        try {
            return getInt(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        try {
            return getLong(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        try {
            return getFloat(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        try {
            return getDouble(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        try {
            return getBigDecimal(result.getIndex(columnLabel) + 1, scale);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        try {
            return getBytes(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        try {
            return getDate(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        try {
            return getTime(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        try {
            return getTimestamp(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        try {
            return getAsciiStream(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        try {
            return getUnicodeStream(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        try {
            return getBinaryStream(result.getIndex(columnLabel) + 1);
        } catch (TarantoolException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(result.getIndex(columnLabel) + 1);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        try {
            return result.getIndex(columnLabel) + 1;
        } catch (TarantoolException e) {
            throw new SQLException(columnLabel + " column is absent", e);
        }

    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(result.getIndex(columnLabel) + 1);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(result.getIndex(columnLabel) + 1);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getType() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public TarantoolStatement getStatement() {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (result.isNull(columnIndex - 1)) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (type == null) {
            throw new SQLException("type must not be null");
        }
        if (type.equals(ByteBuffer.class)) {
            return type.cast(result.getByteBuffer(columnIndex - 1));
        }
        if (type.equals(Boolean.class)) {
            return type.cast(result.getBoolean(columnIndex - 1));
        }
        if (type.equals(Byte.class)) {
            return type.cast((byte) result.getInt(columnIndex - 1));
        }
        if (type.equals(Short.class)) {
            return type.cast((short) result.getInt(columnIndex - 1));
        }
        if (type.equals(Integer.class)) {
            return type.cast(result.getInt(columnIndex - 1));
        }
        if (type.equals(Long.class)) {
            return type.cast(result.getLong(columnIndex - 1));
        }

        if (type.equals(Double.class)) {
            return type.cast(result.getDouble(columnIndex - 1));
        }
        if (type.equals(Float.class)) {
            return type.cast(result.getFloat(columnIndex - 1));
        }
        if (type.equals(String.class)) {
            return type.cast(result.getString(columnIndex - 1));
        }

        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(result.getIndex(columnLabel) + 1, type);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(result.getIndex(columnLabel) + 1, map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(result.getIndex(columnLabel) + 1);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(result.getIndex(columnLabel) + 1);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(result.getIndex(columnLabel) + 1);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(result.getIndex(columnLabel) + 1);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(result.getIndex(columnLabel) + 1, cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(result.getIndex(columnLabel) + 1, cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(result.getIndex(columnLabel) + 1, cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(result.getIndex(columnLabel) + 1);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isClosed() {
        return !result.hasNext();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(result.getIndex(columnLabel) + 1);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(result.getIndex(columnLabel) + 1);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(result.getIndex(columnLabel) + 1);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private static final class ByteBufferBackedInputStream extends InputStream {

        private final ByteBuffer buf;

        ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        public int read() {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        public int read(byte[] bytes, int off, int len) {
            if (bytes == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > bytes.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }
            if (!buf.hasRemaining()) {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    }
}
