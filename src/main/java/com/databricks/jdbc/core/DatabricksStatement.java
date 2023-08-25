package com.databricks.jdbc.core;


import com.databricks.jdbc.client.StatementType;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DatabricksStatement implements IDatabricksStatement, Statement {

  private final DatabricksConnection connection;
  DatabricksResultSet resultSet;
  private String statementId;
  private boolean isClosed;

  public DatabricksStatement(DatabricksConnection connection) {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
    this.isClosed = true;
  }

  @Override
  public String getSessionId() {
    return connection.getSession().getSessionId();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.QUERY);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    executeInternal(
        sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void close() throws SQLException {
    this.isClosed = true;
    this.connection.getSession().getDatabricksClient().closeStatement(statementId);
  }

  @Override
  public void close(boolean removeFromSession) throws SQLException {
    close();
    if (removeFromSession) {
      this.connection.closeStatement(this);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void cancel() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    resultSet = executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.SQL);
    return !resultSet.hasUpdateCount();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException("Not supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  DatabricksResultSet executeInternal(String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    resultSet = connection.getSession().getDatabricksClient().executeStatement(
        sql, connection.getSession().getWarehouseId(), params, statementType, connection.getSession());
    this.isClosed = false;
    return resultSet;
  }
}
