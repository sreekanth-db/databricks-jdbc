package com.databricks.jdbc.pooling;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class VirtualConnection implements Connection {

    private Connection actualConnection;
    private DatabricksPooledConnection pooledConnection;
    private boolean isClosed;

    public VirtualConnection(DatabricksPooledConnection pooledConnection) throws SQLException {
        this.actualConnection = pooledConnection.getActualConnection();
        this.pooledConnection = pooledConnection;
        this.isClosed = actualConnection.isClosed();
    }

    private void throwExceptionIfConnectionIsClosed() throws SQLException {
        if (this.isClosed())
            throw new SQLException("Connection is closed!");
    }

    @Override
    public Statement createStatement() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createStatement();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareStatement(sql);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareCall(sql);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.nativeSQL(sql);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getAutoCommit();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void commit() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.commit();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void rollback() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.rollback();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void close() throws SQLException {
        if (isClosed)
            return;
        pooledConnection.registerConnectionCloseEvent();
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getMetaData();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setReadOnly(readOnly);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.isReadOnly();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setCatalog(catalog);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getCatalog();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setTransactionIsolation(level);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getTransactionIsolation();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getWarnings();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.clearWarnings();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getTypeMap();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setTypeMap(map);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setHoldability(holdability);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getHoldability();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.setSavepoint();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.setSavepoint(name);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.rollback(savepoint);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.releaseSavepoint(savepoint);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareStatement(sql, autoGeneratedKeys);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareStatement(sql, columnIndexes);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.prepareStatement(sql, columnNames);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createClob();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createBlob();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public NClob createNClob() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createNClob();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createSQLXML();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.isValid(timeout);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            this.actualConnection.setClientInfo(name, value);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            this.actualConnection.setClientInfo(properties);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getClientInfo(name);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getClientInfo();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createArrayOf(typeName, elements);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.createStruct(typeName, attributes);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setSchema(schema);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public String getSchema() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getSchema();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.abort(executor);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            this.actualConnection.setNetworkTimeout(executor, milliseconds);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.getNetworkTimeout();
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.unwrap(iface);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throwExceptionIfConnectionIsClosed();
        try {
            return this.actualConnection.isWrapperFor(iface);
        } catch (SQLException e) {
            pooledConnection.registerConnectionErrorEvent(e);
            throw e;
        }
    }
}
