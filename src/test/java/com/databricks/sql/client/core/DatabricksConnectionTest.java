package com.databricks.sql.client.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

import com.databricks.sql.client.core.DatabricksConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

public class DatabricksConnectionTest {

  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";

  public void testConnection() throws Exception {
    DatabricksConnection connection = new DatabricksConnection(DatabricksConnectionContext.parse(JDBC_URL, new Properties()));
    assertTrue(connection.isClosed());
  }
}