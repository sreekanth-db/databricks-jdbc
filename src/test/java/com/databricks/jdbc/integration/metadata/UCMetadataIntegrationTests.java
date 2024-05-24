package com.databricks.jdbc.integration.metadata;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UCMetadataIntegrationTests {

  private Connection connection;

  long currentTimeMillis = System.currentTimeMillis();
  String prefix = "UC_" + currentTimeMillis + "_"; // Define your prefix here
  String catA = prefix + "catA";
  String hiveCatalog = "hive_metastore"; // Define your hive catalog here

  String mainCatalog = "main";
  String db1 = prefix + "db1";
  String db2 = prefix + "db2";
  String hmsDb2Table1 = prefix + "hive_metastore_1";
  String hmsDb2Table2 = prefix + "hive_metastore_2";
  String mainDb1Table1 = prefix + "main_1";

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();

    executeSQL("CREATE CATALOG IF NOT EXISTS " + catA);
    executeSQL("USE CATALOG " + catA);
    executeSQL("CREATE DATABASE IF NOT EXISTS " + db1);
    executeSQL("CREATE DATABASE IF NOT EXISTS " + db2);

    executeSQL("USE " + db1);
    executeSQL(
        "CREATE TABLE IF NOT EXISTS a_1 AS (SELECT 1 AS col_1, '"
            + catA
            + "."
            + db1
            + ".a_1' AS col_2)");
    executeSQL(
        "CREATE TABLE IF NOT EXISTS a_2 AS (SELECT 1 AS col_1, '"
            + catA
            + "."
            + db1
            + ".a_2' AS col_2)");

    executeSQL("USE " + db2);
    executeSQL(
        "CREATE TABLE IF NOT EXISTS a_1 AS (SELECT 1 AS col_1, '"
            + catA
            + "."
            + db2
            + ".a_1' AS col_2)");

    executeSQL("USE CATALOG " + mainCatalog);
    executeSQL("CREATE DATABASE IF NOT EXISTS " + db1);
    executeSQL("CREATE DATABASE IF NOT EXISTS " + db2);

    executeSQL("USE " + db1);
    executeSQL(
        "CREATE TABLE IF NOT EXISTS "
            + mainDb1Table1
            + " AS (SELECT 1 AS col_1, 'main."
            + db1
            + "."
            + mainDb1Table1
            + "' AS col_2)");

    executeSQL("USE CATALOG " + hiveCatalog);
    executeSQL("CREATE DATABASE IF NOT EXISTS " + db2);
    executeSQL("USE " + db2);
    executeSQL(
        "CREATE TABLE IF NOT EXISTS "
            + hmsDb2Table1
            + " AS (SELECT 1 AS col_1, '"
            + hiveCatalog
            + "."
            + db2
            + "."
            + hmsDb2Table1
            + "' AS col_2)");
    executeSQL(
        "CREATE TABLE IF NOT EXISTS "
            + hmsDb2Table2
            + " AS (SELECT 1 AS col_1, '"
            + hiveCatalog
            + "."
            + db2
            + "."
            + hmsDb2Table2
            + "' AS col_2)");

      // Created tables:
      // |     catalog    |    schema   |        table       |
      // +----------------+-------------+--------------------+
      // |      cat_a     |     db1     |         a_1        |
      // |      cat_a     |     db1     |         a_2        |
      // |      cat_a     |     db2     |         a_1        |
      // |      main      |     db1     |       main_1       |
      // |      main      |     db2     |          ∅         |
      // | hive_metastore |     db2     |  hive_metastore_1  |
      // | hive_metastore |     db2     |  hive_metastore_2  |
      // |                | global_temp | global_temp_view_1 |
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }

    // Cleanup
    executeSQL("USE CATALOG " + catA);
    executeSQL("DROP DATABASE " + db1 + " CASCADE");
    executeSQL("DROP DATABASE " + db2 + " CASCADE");
    executeSQL("DROP DATABASE IF EXISTS default CASCADE");
    executeSQL("DROP CATALOG " + catA);

    // Cleanup main catalog
    executeSQL("USE CATALOG " + mainCatalog);
    executeSQL("DROP DATABASE " + db1 + " CASCADE");
    executeSQL("DROP DATABASE " + db2 + " CASCADE");

    // Cleanup legacy catalog
    executeSQL("USE CATALOG " + hiveCatalog);
    executeSQL("DROP DATABASE " + db2 + " CASCADE");
  }

  @Test
  void testGetCatalogs() throws SQLException {
    ResultSet r = connection.getMetaData().getCatalogs();
    verifyContainsCatalogs(r, List.of("main", "hive_metastore", "samples", catA.toLowerCase()));
  }

  @Test
  void testGetSchemas() throws SQLException {
    executeSQL("USE CATALOG hive_metastore");
    ResultSet r = connection.getMetaData().getSchemas("hive_metastore", "*");
    verifyContainsSchemas(
        r,
        List.of(
            List.of("hive_metastore", "default"), List.of("hive_metastore", db2.toLowerCase())));

    r = connection.getMetaData().getSchemas(catA.toLowerCase(), "*");
    verifyContainsSchemas(
        r,
        List.of(
            List.of(catA.toLowerCase(), db1.toLowerCase()),
            List.of(catA.toLowerCase(), db2.toLowerCase()),
            List.of(catA.toLowerCase(), "default")));
  }

  @Test
  void testGetTables() throws SQLException {
    ResultSet r =
        connection.getMetaData().getTables(catA.toLowerCase(), "*", "*", null);
      verifyContainsTables(
              r,
              List.of(
                      List.of(catA.toLowerCase(), db1.toLowerCase(), "a_1", "TABLE"),
                      List.of(catA.toLowerCase(), db1.toLowerCase(), "a_2", "TABLE"),
                      List.of(catA.toLowerCase(), db2.toLowerCase(), "a_1", "TABLE")));

      r = connection.getMetaData().getTables("hive_metastore", "*", "*", null);
      printResultSet(r);
        verifyContainsTables(
                r,
                List.of(
                        List.of("hive_metastore", db2.toLowerCase(), hmsDb2Table1.toLowerCase(), "TABLE"),
                        List.of("hive_metastore", db2.toLowerCase(), hmsDb2Table2.toLowerCase(), "TABLE")));

  }

  @Test
  void testGetColumns() throws SQLException {
    ResultSet r =
        connection.getMetaData().getColumns(catA.toLowerCase(), "*", "*", "*");
    printResultSet(r);
    verifyContainsColumns(
        r,
        List.of(
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_1", "col_1", "4"),
                List.of(catA.toLowerCase(), db1.toLowerCase(), "a_1", "col_2", "12"),
                List.of(catA.toLowerCase(), db1.toLowerCase(), "a_2", "col_1", "4"),
                List.of(catA.toLowerCase(), db1.toLowerCase(), "a_2", "col_2", "12"),
                List.of(catA.toLowerCase(), db2.toLowerCase(), "a_1", "col_1", "4"),
                List.of(catA.toLowerCase(), db2.toLowerCase(), "a_1", "col_2", "12")

                ));

    r =
        connection.getMetaData().getColumns("hive_metastore", "*", "*", "*");
    verifyContainsColumns(
        r,
        List.of(
            List.of("hive_metastore", db2.toLowerCase(), hmsDb2Table1, "col_1", "INTEGER"),
            List.of("hive_metastore", db2.toLowerCase(), hmsDb2Table1, "col_2", "STRING")));
  }


  @Test
  void testGetCurrentCatalogAndSchema() throws SQLException {
    connection = getValidJDBCConnection(List.of(List.of("connCatalog", catA)));
    ResultSet r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    printResultSet(r);
    r.next();
    assert (r.getString(1).equals(catA.toLowerCase()));
    assert (r.getString(2).equals("default"));

    connection = getValidJDBCConnection(List.of(List.of("connCatalog", "samples")));
    r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    printResultSet(r);
    r.next();
    assert (r.getString(1).equals("samples"));
    assert (r.getString(2).equals("default"));

    connection = getValidJDBCConnection(List.of(List.of("connSchema", db2.toLowerCase())));
    r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    printResultSet(r);
    r.next();
    assert (r.getString(1).equals("hive_metastore"));
    assert (r.getString(2).equals(db2.toLowerCase()));

    connection = getValidJDBCConnection(List.of(List.of("connCatalog", "fake_catalog"), List.of("connSchema", "fake_schema")));
    r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    printResultSet(r);
    r.next();
    assert (r.getString(1).equals("fake_catalog"));
    assert (r.getString(2).equals("fake_schema"));
  }


    private void verifyContainsColumns(ResultSet r, List<List<String>> included_columns) {
        ArrayList<List<String>> allColumns = new ArrayList<>();
        try {
            while (r.next()) {
                allColumns.add(
                        List.of(
                                r.getString("TABLE_CAT"),
                                r.getString("TABLE_SCHEM"),
                                r.getString("TABLE_NAME"),
                                r.getString("COLUMN_NAME"),
                                r.getString("DATA_TYPE")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assert (allColumns.containsAll(included_columns));
    }

    private void verifyContainsTables(ResultSet r, List<List<String>> included_tables) {
        ArrayList<List<String>> allTables = new ArrayList<>();
        try {
          while (r.next()) {
            allTables.add(
                List.of(
                    r.getString("TABLE_CAT"),
                    r.getString("TABLE_SCHEM"),
                    r.getString("TABLE_NAME"),
                    r.getString("TABLE_TYPE")));
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
        assert (allTables.containsAll(included_tables));
  }

  private void verifyContainsSchemas(ResultSet r, List<List<String>> included_schema) {
    ArrayList<List<String>> allSchemas = new ArrayList<>();
    try {
      while (r.next()) {
        allSchemas.add(List.of(r.getString("TABLE_CATALOG"), r.getString("TABLE_SCHEM")));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    assert (allSchemas.containsAll(included_schema));
  }

  private void verifyContainsCatalogs(ResultSet r, List<String> included_catalogs) {
    ArrayList<String> allCatalogs = new ArrayList<>();
    try {
      while (r.next()) {
        allCatalogs.add(r.getString("TABLE_CAT"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    assert (allCatalogs.containsAll(included_catalogs));
  }

  public void printResultSet(ResultSet resultSet) throws SQLException {
    System.out.println("\n\nPrinting resultSet...........\n");
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnName(i) + "\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnTypeName(i) + "\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnType(i) + "\t\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getPrecision(i) + "\t\t\t");
    System.out.println();
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        try {
          Object columnValue = resultSet.getObject(i);
          System.out.print(columnValue + "\t\t");
        } catch (Exception e) {
          System.out.print(
              "NULL\t\t"); // It is possible for certain columns to be non-existent (edge case)
        }
      }
      System.out.println();
    }
  }
}
