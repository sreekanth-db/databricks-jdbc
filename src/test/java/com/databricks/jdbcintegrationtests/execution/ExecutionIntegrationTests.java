package com.databricks.jdbcintegrationtests.execution;

import static com.databricks.jdbcintegrationtests.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ExecutionIntegrationTests {

  @BeforeAll
  static void setUp() {}

  @Test
  void testInsertStatement() throws SQLException {
    String tableName = "insert_test_table";
    setUpDatabaseSchema(tableName);

    String SQL =
        "INSERT INTO "
            + getDatabricksCatalog()
            + "."
            + tableName
            + " (id, column1, column2) VALUES (1, 'value1', 'value2')";
    assertDoesNotThrow(() -> executeSQL(SQL), "Error executing SQL");

    ResultSet rs = executeQuery("SELECT * FROM " + getDatabricksCatalog() + "." + tableName);
    int rows = 0;
    while (rs != null && rs.next()) {
      rows++;
    }
    assertTrue(rows == 1, "Expected 1 row, got " + rows);
  }
}
