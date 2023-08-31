package com.databricks.jdbc.pooling;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class DatabricksPooledConnection implements PooledConnection {

    private Connection actualConnection;
    private final Set<ConnectionEventListener> eventListeners;

    public DatabricksPooledConnection(Connection actualConnection) {
        this.actualConnection = actualConnection;
        this.eventListeners = new HashSet<>();
    }

    public Connection getActualConnection() {
        return this.actualConnection;
    }

    public void registerConnectionErrorEvent(SQLException e) {
        for (ConnectionEventListener eventListener : eventListeners) {
            eventListener.connectionErrorOccurred(new ConnectionEvent(this, e));
        }
    }

    public void registerConnectionCloseEvent() {
        for (ConnectionEventListener eventListener : eventListeners) {
            eventListener.connectionClosed(new ConnectionEvent(this));
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new VirtualConnection(this);
    }

    @Override
    public void close() throws SQLException {
        if (this.actualConnection != null) {
            this.actualConnection.close();
            this.actualConnection = null;
        }
        eventListeners.clear();
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        this.eventListeners.add(listener);
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        this.eventListeners.remove(listener);
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {

    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {

    }
}
