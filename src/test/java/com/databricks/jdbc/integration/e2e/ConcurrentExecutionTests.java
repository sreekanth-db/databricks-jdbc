package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

public class ConcurrentExecutionTests {
  private static final int NUM_THREADS = 20;

  @Test
  void testConcurrentExecution() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Boolean>> futures = new ArrayList<>();

    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadNum = i;
      Future<Boolean> future =
          executorService.submit(
              () -> {
                try {
                  runThreadQueries(threadNum);
                  return true;
                } catch (Exception e) {
                  e.printStackTrace();
                  return false;
                }
              });
      futures.add(future);
    }

    executorService.shutdown();

    boolean allSuccess = true;
    for (Future<Boolean> future : futures) {
      try {
        if (!future.get()) {
          allSuccess = false;
        }
      } catch (ExecutionException e) {
        e.printStackTrace();
        allSuccess = false;
      }
    }

    assertTrue(allSuccess, "Not all threads completed successfully");
  }

  private void runThreadQueries(int threadNum) throws SQLException {
    try (Connection connection = getValidJDBCConnection()) {
      // Use a unique table name per thread to avoid conflicts
      String tableName = "concurrent_test_table_" + threadNum;
      setupDatabaseTable(connection, tableName);

      // Insert data
      String insertSQL =
          "INSERT INTO "
              + getFullyQualifiedTableName(tableName)
              + " (id, col1, col2) VALUES ("
              + threadNum
              + ", 'value"
              + threadNum
              + "', 'value"
              + threadNum
              + "')";

      try (Statement statement = connection.createStatement()) {
        statement.execute(insertSQL);
      }

      // Update data
      String updateSQL =
          "UPDATE "
              + getFullyQualifiedTableName(tableName)
              + " SET col1 = 'updatedValue"
              + threadNum
              + "' WHERE id = "
              + threadNum;
      try (Statement statement = connection.createStatement()) {
        statement.execute(updateSQL);
      }

      // Select data
      String selectSQL =
          "SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = " + threadNum;
      try (Statement statement = connection.createStatement()) {
        try (ResultSet rs = statement.executeQuery(selectSQL)) {
          if (rs.next()) {
            String col1 = rs.getString("col1");
            assertEquals(
                "updatedValue" + threadNum, col1, "Expected updated value in thread " + threadNum);
          } else {
            fail("No data found in thread " + threadNum);
          }
        }
      }

      // Delete data
      String deleteSQL =
          "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = " + threadNum;
      try (Statement statement = connection.createStatement()) {
        statement.execute(deleteSQL);
      }

      // Clean up table
      deleteTable(connection, tableName);
    }
  }

  @Test
  void testConcurrentStatementExecution()
      throws SQLException, ExecutionException, InterruptedException {

    try (Connection connection = getValidJDBCConnection()) {
      ExecutorService executor = Executors.newFixedThreadPool(10);
      List<Future<Boolean>> futures = new ArrayList<>();

      // Multiple threads creating and using statements on same connection
      for (int i = 0; i < 20; i++) {
        final int threadId = i;
        futures.add(
            executor.submit(
                () -> {
                  try {
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT " + threadId + " as thread_id");
                    boolean hasResult = rs.next();
                    int result = rs.getInt("thread_id");
                    rs.close();
                    stmt.close();
                    return hasResult && result == threadId;
                  } catch (SQLException e) {
                    e.printStackTrace();
                    return false;
                  }
                }));
      }

      // Verify all threads succeeded
      for (Future<Boolean> future : futures) {
        assertTrue(future.get(), "Statement execution failed");
      }
    }
  }

  @Test
  void testConcurrentInsertAndCount() throws InterruptedException, SQLException {

    String sharedTableName = "shared_insert_table";

    try (Connection setupConn = getValidJDBCConnection()) {
      setupDatabaseTable(setupConn, sharedTableName);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Boolean>> futures = new ArrayList<>();

    // Each thread inserts one row
    for (int i = 0; i < NUM_THREADS; i++) {
      final int threadId = i;
      futures.add(
          executorService.submit(
              () -> {
                try (Connection conn = getValidJDBCConnection()) {
                  conn.createStatement()
                      .executeUpdate(
                          "INSERT INTO "
                              + getFullyQualifiedTableName(sharedTableName)
                              + " (id, col1, col2) VALUES ("
                              + threadId
                              + ", 'thread_"
                              + threadId
                              + "', 'data')");
                  return true;
                } catch (Exception e) {
                  return false;
                }
              }));
    }

    executorService.shutdown();

    // Verify all threads succeeded
    boolean allSuccess = true;
    for (Future<Boolean> future : futures) {
      try {
        if (!future.get()) allSuccess = false;
      } catch (ExecutionException e) {
        allSuccess = false;
      }
    }
    assertTrue(allSuccess, "Not all threads completed successfully");

    // Verify row count = NUM_THREADS
    try (Connection verifyConn = getValidJDBCConnection()) {
      ResultSet rs =
          verifyConn
              .createStatement()
              .executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName(sharedTableName));
      rs.next();
      int rowCount = rs.getInt(1);
      assertEquals(NUM_THREADS, rowCount, "Row count should equal number of threads");
    }

    // Cleanup
    try (Connection cleanupConn = getValidJDBCConnection()) {
      deleteTable(cleanupConn, sharedTableName);
    }
  }
}
