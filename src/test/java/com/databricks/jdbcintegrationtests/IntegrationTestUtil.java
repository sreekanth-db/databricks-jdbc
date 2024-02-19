package com.databricks.jdbcintegrationtests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Utility class to support integration tests * */
public class IntegrationTestUtil {

  public static String getDatabricksHost() {
    // includes port
    return System.getenv("DATABRICKS_HOST");
  }

  public static String getDatabricksToken() {
    return System.getenv("DATABRICKS_TOKEN");
  }

  public static String getDatabricksHTTPPath() {
    return System.getenv("DATABRICKS_HTTP_PATH");
  }

  public static String getDatabricksCatalog() {
    return System.getenv("DATABRICKS_CATALOG");
  }

  public static String getDatabricksUser() {
    return System.getenv("DATABRICKS_USER");
  }

  public static Connection getValidJDBCConnection() throws SQLException {
    // add support for properties
    return DriverManager.getConnection(getJDBCUrl(), getDatabricksUser(), getDatabricksToken());
  }

  public static String getJDBCUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksHost();
    String httpPath = getDatabricksHTTPPath();

    return String.format(template, host, httpPath);
  }
}
