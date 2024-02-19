package com.databricks.jdbc.integration;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionIntegrationTests {

    @Test
    void testSuccessfulConnection() throws SQLException {
       Connection conn = IntegrationTestUtil.getValidJDBCConnection();
       assert ((conn != null) && !conn.isClosed());

       if(conn != null) conn.close();
    }
}
