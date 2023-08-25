package com.databricks.jdbc.core;


import com.databricks.jdbc.client.StatementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DatabricksStatement implements IDatabricksStatement, Statement {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksStatement.class);
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
    LOGGER.debug("public void close()");
    this.isClosed = true;
    this.connection.getSession().getDatabricksClient().closeStatement(statementId);
  }

  @Override
  public void close(boolean removeFromSession) throws SQLException {
    LOGGER.debug("public void close(boolean removeFromSession)");
    close();
    if (removeFromSession) {
      this.connection.closeStatement(this);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    LOGGER.debug("public int getMaxFieldSize()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    LOGGER.debug("public void setMaxFieldSize(int max = {})", max);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getMaxRows() throws SQLException {
    LOGGER.debug("public int getMaxRows()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    LOGGER.debug("public void setMaxRows(int max = {})", max);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    LOGGER.debug("public void setEscapeProcessing(boolean enable = {})", enable);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    LOGGER.debug("public int getQueryTimeout()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    LOGGER.debug("public void setQueryTimeout(int seconds = {})", seconds);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void cancel() throws SQLException {
    LOGGER.debug("public void cancel()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    LOGGER.debug("public SQLWarning getWarnings()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearWarnings() throws SQLException {
    LOGGER.debug("public void clearWarnings()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    LOGGER.debug("public void setCursorName(String name = {})", name);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    resultSet = executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.SQL);
    return !resultSet.hasUpdateCount();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    LOGGER.debug("public ResultSet getResultSet()");
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    LOGGER.debug("public int getUpdateCount()");
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    LOGGER.debug("public boolean getMoreResults()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    LOGGER.debug("public void setFetchDirection(int direction = {})", direction);
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException("Not supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    LOGGER.debug("public int getFetchDirection()");
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    LOGGER.debug("public void setFetchSize(int rows = {})", rows);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getFetchSize() throws SQLException {
    LOGGER.debug("public int getFetchSize()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    LOGGER.debug("public int getResultSetConcurrency()");
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    LOGGER.debug("public int getResultSetType()");
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    LOGGER.debug("public void addBatch(String sql = {})", sql);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearBatch() throws SQLException {
    LOGGER.debug("public void clearBatch()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    LOGGER.debug("public int[] executeBatch()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Connection getConnection() throws SQLException {
    LOGGER.debug("public Connection getConnection()");
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    LOGGER.debug("public boolean getMoreResults(int current = {})", current);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    LOGGER.debug("public ResultSet getGeneratedKeys()");
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
    LOGGER.debug("public int executeUpdate(String sql, String[] columnNames)");
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
    LOGGER.debug("public int getResultSetHoldability()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isClosed() throws SQLException {
    LOGGER.debug("public boolean isClosed()");
    return this.isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    LOGGER.debug("public void setPoolable(boolean poolable = {})", poolable);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    LOGGER.debug("public boolean isPoolable()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    LOGGER.debug("public void closeOnCompletion()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    LOGGER.debug("public boolean isCloseOnCompletion()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface)");
    throw new UnsupportedOperationException("Not implemented");
  }

  DatabricksResultSet executeInternal(String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType) throws SQLException {
    LOGGER.debug("DatabricksResultSet executeInternal(String sql = {}, Map<Integer, ImmutableSqlParameter> params = {}, StatementType statementType = {})", sql, params, statementType);
    resultSet = connection.getSession().getDatabricksClient().executeStatement(
            sql, connection.getSession().getWarehouseId(), params, statementType, connection.getSession());
    this.isClosed = false;
    return resultSet;
  }
}