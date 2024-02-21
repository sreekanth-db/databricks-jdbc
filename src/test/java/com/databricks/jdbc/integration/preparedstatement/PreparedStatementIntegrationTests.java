package com.databricks.jdbc.integration.preparedstatement;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PreparedStatementIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  private static void insertTestData(String tableName) throws SQLException {
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(insertSQL);
  }

  @Test
  void testPreparedStatementExecution() throws SQLException {
    String tableName = "prepared_statement_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);
    String sql = "SELECT * FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 1); // Assuming 'id' is an integer
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
      }
    }
    deleteTable(tableName);
  }

  @Test
  void testParameterBindingInPreparedStatement() throws SQLException {
    String tableName = "parameter_binding_test_table";
    setupDatabaseTable(tableName);
    String sql =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + "(id, col1, col2) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 2);
      statement.setString(2, "value1");
      statement.setString(3, "value2");
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted");
    }
    deleteTable(tableName);
  }

  @Test
  void testPreparedStatementComplexQueryExecution() throws SQLException {
    String tableName = "prepared_statement_complex_query_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);
    String sql = "UPDATE " + getFullyQualifiedTableName(tableName) + " SET col1 = ? WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, "Updated value");
      statement.setInt(2, 1);
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be updated");
    }
    deleteTable(tableName);
  }

  @Test
  void testHandlingNullValuesWithPreparedStatement() throws SQLException {
    String tableName = "prepared_statement_null_handling_test_table";
    setupDatabaseTable(tableName);
    String sql =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + "(id, col1, col2) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, 6);
      statement.setNull(2, java.sql.Types.VARCHAR);
      statement.setString(3, "value1");
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted with a null col1");
    }
    deleteTable(tableName);
  }
}
