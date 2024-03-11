package com.databricks.jdbc.integration.benchmarking;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

import java.sql.*;
import java.util.Enumeration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetadataBenchmarkingTest {

  private Connection connection;
  private static final int NUM_SCHEMAS = 10;
  private static final int NUM_TABLES = 2;
  private static final int NUM_COLUMNS = 1;

  private static final String BASE_SCHEMA_NAME = "jdbc_metadata_benchmark_schema";

  private static final String BASE_TABLE_NAME = "table";

  Long[][][] durationPerTable = new Long[2][NUM_SCHEMAS][NUM_TABLES];

  Long[][] durationPerSchema = new Long[2][NUM_SCHEMAS];

  private double[] avgTimePerTable = new double[2];

  private double[] avgTimePerSchema = new double[2];

  @BeforeEach
  void setUp() throws SQLException, ClassNotFoundException {
    connection = getValidJDBCConnection();
    setUpSchemas();
    setUpTables();
  }

  @AfterEach
  void tearDown() throws SQLException {
    connection = getValidJDBCConnection();
    tearDownSchemas();
  }

  private void setUpSchemas() {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      System.out.println("Creating schema " + i);
      executeSQL("CREATE SCHEMA IF NOT EXISTS " + getDatabricksCatalog() + "." + BASE_SCHEMA_NAME + i);
    }
  }

  private void setUpTables() {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      for (int j = 0; j < NUM_TABLES; j++) {
        System.out.println("Creating table " + j + " in schema " + i);
        executeSQL(
            "CREATE TABLE IF NOT EXISTS "
                + getDatabricksCatalog()
                + "." + BASE_SCHEMA_NAME
                + i
                + "." + BASE_TABLE_NAME
                + j
                + " "
                + getColumnString());
      }
    }
  }

  private String getColumnString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(id INT");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      sb.append(", col" + i + " STRING");
    }
    sb.append(")");
    return sb.toString();
  }

  private void tearDownSchemas() {
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      executeSQL("DROP SCHEMA IF EXISTS " + getDatabricksCatalog() + "." + BASE_SCHEMA_NAME + i + " CASCADE");
    }
  }

  void measureMetadataPerformance(int recording) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    metaData.getSchemas(getDatabricksCatalog(), "schema%");
    for(int i = 0; i < 100; i++) {
      metaData.getTables(getDatabricksCatalog(), "%", "%", null);
    }
    for (int i = 0; i < NUM_SCHEMAS; i++) {
      System.out.println("Starting schema " + i);
      long startTimeForSchemas = System.currentTimeMillis();
      metaData.getSchemas(getDatabricksCatalog(), BASE_SCHEMA_NAME + i);
      metaData.getTables(getDatabricksCatalog(), BASE_SCHEMA_NAME + i, BASE_TABLE_NAME, null);
      for (int j = 0; j < NUM_TABLES; j++) {
        long startTimeForTables = System.currentTimeMillis();
        metaData.getTables(getDatabricksCatalog(), BASE_SCHEMA_NAME + i, BASE_TABLE_NAME + j, null);
        metaData.getColumns(getDatabricksCatalog(), BASE_SCHEMA_NAME + i, BASE_TABLE_NAME + j, null);
        long endTimeForTables = System.currentTimeMillis();
        durationPerTable[recording][i][j] = endTimeForTables - startTimeForTables;
        avgTimePerTable[recording] += durationPerTable[recording][i][j];
      }
      long endTimeForSchemas = System.currentTimeMillis();
      durationPerSchema[recording][i] = endTimeForSchemas - startTimeForSchemas;
      avgTimePerSchema[recording] += durationPerSchema[recording][i];
      System.out.println("Completed schema " + i);
    }
    avgTimePerSchema[recording] /= NUM_SCHEMAS;
    avgTimePerTable[recording] /= (NUM_SCHEMAS * NUM_TABLES);

  }

  @Test
  void benchmarkDrivers() throws SQLException {
    // Currently connection is held by OSS driver
    long startTime = System.currentTimeMillis();
    measureMetadataPerformance(0);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken by OSS JDBC: " + (endTime - startTime) + "ms");

    // Switching to Databricks JDBC
    connection.close();
    DriverManager.deregisterDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    connection = DriverManager.getConnection(getJDBCUrl(), "token", getDatabricksToken());

    // Currently connection is held by Databricks driver
    startTime = System.currentTimeMillis();
    measureMetadataPerformance(1);
    endTime = System.currentTimeMillis();
    System.out.println("Time taken by Databricks JDBC: " + (endTime - startTime) + "ms");
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.deregisterDriver(new com.databricks.client.jdbc.Driver());


    System.out.println("STATS");
    System.out.println("Total number of schemas: " + NUM_SCHEMAS);
    System.out.println("Total number of tables per schema: " + NUM_TABLES);
    System.out.println("Total number of tables: " + NUM_SCHEMAS * NUM_TABLES);
    System.out.println("Total number of columns per table: " + NUM_COLUMNS + 1);
    System.out.println("Average time taken by OSS JDBC per schema: " + avgTimePerSchema[0] + "ms");
    System.out.println("Average time taken by Databricks JDBC per schema: " + avgTimePerSchema[1] + "ms");
    System.out.println("Average time taken by OSS JDBC per table: " + avgTimePerTable[0] + "ms");
    System.out.println("Average time taken by Databricks JDBC per table: " + avgTimePerTable[1] + "ms");
  }
}
