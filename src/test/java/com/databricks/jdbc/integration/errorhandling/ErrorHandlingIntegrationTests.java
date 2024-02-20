package com.databricks.jdbc.integration.errorhandling;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ErrorHandlingIntegrationTests {

  String tableName = "test_table";

  @AfterEach
  void cleanUp() throws SQLException {
    String SQL =
        "DROP TABLE IF EXISTS "
            + getDatabricksCatalog()
            + "."
            + getDatabricksSchema()
            + "."
            + tableName;
    executeSQL(SQL);
  }

  @Test
  void testFailureToLoadDriver() {
    Exception exception =
        assertThrows(
            ClassNotFoundException.class,
            () -> {
              Class.forName("incorrect.DatabricksDriver.class");
            });
    assertTrue(exception.getMessage().contains("incorrect.DatabricksDriver.class"));
  }

  @Test
  void testInvalidURL() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              Connection connection =
                  getConnection("jdbc:abcde://invalidhost:0000/db", "username", "password");
            });
    assertTrue(exception.getMessage().contains("Invalid url"));
  }

  @Test
  void testInvalidHostname() {
    assertThrows(
        SQLException.class,
        () -> {
          Connection connection =
              getConnection(
                  "jdbc:databricks://e2-wrongfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;",
                  "username",
                  "password");
        });
  }

  @Test
  void testQuerySyntaxError() {
    setUpDatabaseSchema(tableName);
    assertThrows(
        SQLException.class,
        () -> {
          Connection connection = getValidJDBCConnection();
          Statement statement = connection.createStatement();
          String sql =
              "INSER INTO "
                  + getDatabricksCatalog()
                  + "."
                  + getDatabricksSchema()
                  + "."
                  + tableName
                  + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
          statement.executeQuery(sql);
        });
  }

  @Test
  void testAccessingClosedResultSet() {
    setUpDatabaseSchema(tableName);
    executeSQL(
        "INSERT INTO "
            + getDatabricksCatalog()
            + "."
            + getDatabricksSchema()
            + "."
            + tableName
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')");
    ResultSet resultSet =
        executeQuery(
            "SELECT * FROM "
                + getDatabricksCatalog()
                + "."
                + getDatabricksSchema()
                + "."
                + tableName);
    try {
      resultSet.close();
      assertThrows(SQLException.class, resultSet::next);
    } catch (SQLException e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  void testCallingUnsupportedSQLFeature() {
    setUpDatabaseSchema(tableName);
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          Connection connection = getValidJDBCConnection();
          Statement statement = connection.createStatement();
          String sql =
              "SELECT * FROM "
                  + getDatabricksCatalog()
                  + "."
                  + getDatabricksSchema()
                  + "."
                  + tableName;
          ResultSet resultSet = statement.executeQuery(sql);
          resultSet.first(); // Currently unsupported method
        });
  }

  private Connection getConnection(String url, String username, String password)
      throws SQLException {
    return DriverManager.getConnection(url, username, password);
  }
}
