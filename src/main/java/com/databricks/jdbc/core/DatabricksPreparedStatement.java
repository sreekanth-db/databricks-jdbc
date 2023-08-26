package com.databricks.jdbc.core;

import com.databricks.jdbc.client.StatementType;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class DatabricksPreparedStatement extends DatabricksStatement implements PreparedStatement {

  private final String sql;
  private final Map<Integer, ImmutableSqlParameter> parameterBindings;

  public DatabricksPreparedStatement(DatabricksConnection connection, String sql) {
    super(connection);
    this.sql = sql;
    this.parameterBindings = new HashMap<Integer, ImmutableSqlParameter>();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return executeInternal(sql, parameterBindings, StatementType.QUERY);
  }

  @Override
  public int executeUpdate() throws SQLException {
    executeInternal(sql, parameterBindings, StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.BOOLEAN);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.TINYINT);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.SMALLINT);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.INTEGER);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.BIGINT);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.FLOAT);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.DOUBLE);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.DECIMAL);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.VARCHAR);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.DATE);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.TIME);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.TIMESTAMP);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearParameters() throws SQLException {
    checkIfClosed();
    this.parameterBindings.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    checkIfClosed();
    this.parameterBindings.put(parameterIndex, ImmutableSqlParameter.builder()
        .type(targetSqlType)
        .value(x)
        .cardinal(parameterIndex)
        .build());
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    checkIfClosed();
    setObject(parameterIndex, x, Types.JAVA_OBJECT);
  }

  @Override
  public boolean execute() throws SQLException {
    checkIfClosed();
    executeInternal(sql, parameterBindings, StatementType.SQL);
    return !resultSet.hasUpdateCount();
  }

  @Override
  public void addBatch() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    checkIfClosed();
    return resultSet.getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }
}
