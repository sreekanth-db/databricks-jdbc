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
  private boolean closeOnCompletion;

  public DatabricksStatement(DatabricksConnection connection) {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
    this.isClosed = false;
  }

  @Override
  public String getSessionId() {
    return connection.getSession().getSessionId();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    checkIfClosed();
    return executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.QUERY);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkIfClosed();
    executeInternal(
        sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void close() throws SQLException {
    close(true);
  }

  @Override
  public void close(boolean removeFromSession) throws SQLException {
    this.isClosed = true;
    this.connection.getSession().getDatabricksClient().closeStatement(statementId);
    if (resultSet != null) {
      this.resultSet.close();
      this.resultSet = null;
    }
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
    checkIfClosed();
    resultSet = executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.SQL);
    return !resultSet.hasUpdateCount();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkIfClosed();
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    checkIfClosed();
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException("Not supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkIfClosed();
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
    checkIfClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    checkIfClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException("Method not supported", "addBatch(String sql)");
  }

  @Override
  public void clearBatch() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException("Method not supported", "clearBatch()");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException("Method not supported", "executeBatch()");
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
    checkIfClosed();
    return new EmptyResultSet();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeUpdate(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported", "executeUpdate(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "executeUpdate(String sql, int[] columnIndexes)");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "executeUpdate(String sql, String[] columnNames)");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported", "execute(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "execute(String sql, int[] columnIndexes)");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "execute(String sql, String[] columnNames)");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    checkIfClosed();
    if (poolable) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported", "setPoolable(boolean poolable)");
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    checkIfClosed();
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    checkIfClosed();
    this.closeOnCompletion = true;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    checkIfClosed();
    return this.closeOnCompletion;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void handleResultSetClose(IDatabricksResultSet resultSet) throws SQLException {
    // Don't throw exception, we are already closing here
    if (closeOnCompletion) {
      this.close(true);
    }
  }

  DatabricksResultSet executeInternal(String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    resultSet = connection.getSession().getDatabricksClient().executeStatement(
        sql, connection.getSession().getWarehouseId(), params, statementType, connection.getSession(), this);
    this.isClosed = false;
    return resultSet;
  }

  void checkIfClosed() throws SQLException {
    if (isClosed) {
      throw new DatabricksSQLException("Statement is closed");
    }
  }
}
