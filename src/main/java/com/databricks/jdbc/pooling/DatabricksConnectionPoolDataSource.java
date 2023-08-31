package com.databricks.jdbc.pooling;

import com.databricks.jdbc.core.DatabricksDataSource;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import java.sql.SQLException;

public class DatabricksConnectionPoolDataSource extends DatabricksDataSource implements ConnectionPoolDataSource {
    @Override
    public PooledConnection getPooledConnection() throws SQLException {
        return new DatabricksPooledConnection(super.getConnection());
    }

    @Override
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        return new DatabricksPooledConnection(super.getConnection(user, password));
    }
}
