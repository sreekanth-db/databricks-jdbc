package com.databricks.jdbcintegrationtests.connection;

import java.sql.Connection;
import java.sql.SQLException;

import com.databricks.jdbcintegrationtests.IntegrationTestUtil;
import org.junit.jupiter.api.Test;

public class ConnectionIntegrationTests {

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = IntegrationTestUtil.getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    if (conn != null) conn.close();
  }
}
