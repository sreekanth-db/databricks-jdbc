package com.databricks.jdbc.pooling;

import com.databricks.jdbc.core.DatabricksDataSource;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

public class DatabricksConnectionPoolDataSource extends DatabricksDataSource
    implements ConnectionPoolDataSource {
  @Override
  public PooledConnection getPooledConnection() {
    return new DatabricksPooledConnection(super.getConnection());
  }

  @Override
  public PooledConnection getPooledConnection(String user, String password) {
    return new DatabricksPooledConnection(super.getConnection(user, password));
  }
}
