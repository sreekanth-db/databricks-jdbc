package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for Query Tags functionality. Tests that QUERY_TAGS parameter is properly passed to the
 * backend and queries execute successfully with tags attached.
 */
public class QueryTagsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryTagsTest.class);

  @Test
  void testQueryTags() throws SQLException {
    String queryTags = "key1:value1,driver:jdbc";

    try (Connection connection =
        getValidJDBCConnection(List.of(List.of("QUERY_TAGS", queryTags)))) {

      Properties sessionConfigs = connection.getClientInfo();
      assertTrue(sessionConfigs.containsKey("query_tags"));
      assertEquals(queryTags, sessionConfigs.get("query_tags"));

      ResultSet rs = executeQuery(connection, "SELECT 1");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));

      LOGGER.info(
          "Backend accepted QUERY_TAGS parameter during session creation and statement executed successfully");
    }
  }
}
