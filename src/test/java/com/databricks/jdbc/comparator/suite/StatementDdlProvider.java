package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compares DDL operations (CREATE, ALTER, DROP) between Thrift and SEA connections.
 *
 * <p>Each connection operates on its own namespace (suffixed _thrift / _sea) inside the
 * comparator_ddl_tests catalog to avoid interference. Compares executeUpdate return values,
 * exception behavior, and side effects (object existence).
 */
public class StatementDdlProvider implements SuiteProvider {

  private static final String CATALOG = "comparator_ddl_tests";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase("CREATE_SCHEMA", "CREATE SCHEMA"),
        new TestCase("CREATE_TABLE", "CREATE TABLE with multiple column types"),
        new TestCase("ALTER_TABLE_ADD_COLUMN", "ALTER TABLE ADD COLUMN"),
        new TestCase("DROP_TABLE", "DROP TABLE"),
        new TestCase("DROP_SCHEMA", "DROP SCHEMA CASCADE"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String op = testCase.getIdentifier();
    String schema1 = CATALOG + ".ddl_thrift";
    String schema2 = CATALOG + ".ddl_sea";
    String table1 = schema1 + ".test_table";
    String table2 = schema2 + ".test_table";

    List<String> differences = new ArrayList<>();

    // Clean slate: drop schemas from any previous run
    exec(conn1, "DROP SCHEMA IF EXISTS " + schema1 + " CASCADE");
    exec(conn2, "DROP SCHEMA IF EXISTS " + schema2 + " CASCADE");

    switch (op) {
      case "CREATE_SCHEMA":
        compareDdl(
            conn1,
            conn2,
            "CREATE SCHEMA IF NOT EXISTS " + schema1,
            "CREATE SCHEMA IF NOT EXISTS " + schema2,
            differences);
        // Verify: schema exists (1 row each)
        verifySideEffect(
            conn1,
            conn2,
            "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_thrift'",
            "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_sea'",
            1,
            differences,
            "verify schema exists");
        break;

      case "CREATE_TABLE":
        // Ensure schema exists
        exec(conn1, "CREATE SCHEMA IF NOT EXISTS " + schema1);
        exec(conn2, "CREATE SCHEMA IF NOT EXISTS " + schema2);
        compareDdl(
            conn1,
            conn2,
            "CREATE TABLE IF NOT EXISTS "
                + table1
                + " (id INT, name STRING, value DOUBLE, active BOOLEAN)",
            "CREATE TABLE IF NOT EXISTS "
                + table2
                + " (id INT, name STRING, value DOUBLE, active BOOLEAN)",
            differences);
        // Verify: table has 4 columns
        verifySideEffect(
            conn1,
            conn2,
            "DESCRIBE TABLE " + table1,
            "DESCRIBE TABLE " + table2,
            4,
            differences,
            "verify table created with 4 columns");
        break;

      case "ALTER_TABLE_ADD_COLUMN":
        // Ensure table exists
        exec(conn1, "CREATE SCHEMA IF NOT EXISTS " + schema1);
        exec(conn2, "CREATE SCHEMA IF NOT EXISTS " + schema2);
        exec(
            conn1,
            "CREATE TABLE IF NOT EXISTS "
                + table1
                + " (id INT, name STRING, value DOUBLE, active BOOLEAN)");
        exec(
            conn2,
            "CREATE TABLE IF NOT EXISTS "
                + table2
                + " (id INT, name STRING, value DOUBLE, active BOOLEAN)");
        compareDdl(
            conn1,
            conn2,
            "ALTER TABLE " + table1 + " ADD COLUMNS (extra_col STRING)",
            "ALTER TABLE " + table2 + " ADD COLUMNS (extra_col STRING)",
            differences);
        // Verify: table now has 5 columns (4 original + 1 added)
        verifySideEffect(
            conn1,
            conn2,
            "DESCRIBE TABLE " + table1,
            "DESCRIBE TABLE " + table2,
            5,
            differences,
            "verify column added");
        break;

      case "DROP_TABLE":
        // Ensure table exists
        exec(conn1, "CREATE SCHEMA IF NOT EXISTS " + schema1);
        exec(conn2, "CREATE SCHEMA IF NOT EXISTS " + schema2);
        exec(
            conn1,
            "CREATE TABLE IF NOT EXISTS "
                + table1
                + " (id INT, name STRING, value DOUBLE, active BOOLEAN)");
        exec(
            conn2,
            "CREATE TABLE IF NOT EXISTS "
                + table2
                + " (id INT, name STRING, value DOUBLE, active BOOLEAN)");
        compareDdl(
            conn1,
            conn2,
            "DROP TABLE IF EXISTS " + table1,
            "DROP TABLE IF EXISTS " + table2,
            differences);
        // Verify: table gone (0 rows)
        verifySideEffect(
            conn1,
            conn2,
            "SHOW TABLES IN " + schema1 + " LIKE 'test_table'",
            "SHOW TABLES IN " + schema2 + " LIKE 'test_table'",
            0,
            differences,
            "verify table dropped");
        break;

      case "DROP_SCHEMA":
        // Ensure schema exists with a table
        exec(conn1, "CREATE SCHEMA IF NOT EXISTS " + schema1);
        exec(conn2, "CREATE SCHEMA IF NOT EXISTS " + schema2);
        exec(conn1, "CREATE TABLE IF NOT EXISTS " + table1 + " (id INT)");
        exec(conn2, "CREATE TABLE IF NOT EXISTS " + table2 + " (id INT)");
        compareDdl(
            conn1,
            conn2,
            "DROP SCHEMA IF EXISTS " + schema1 + " CASCADE",
            "DROP SCHEMA IF EXISTS " + schema2 + " CASCADE",
            differences);
        // Verify: schema gone (0 rows)
        verifySideEffect(
            conn1,
            conn2,
            "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_thrift'",
            "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_sea'",
            0,
            differences,
            "verify schema dropped");
        break;

      default:
        throw new IllegalArgumentException("Unknown DDL operation: " + op);
    }

    ComparisonResult result = new ComparisonResult(label, op, testCase.getArgs());
    result.dataDifferences = differences;
    return result;
  }

  /** Executes DDL on both connections and compares return values and exceptions. */
  private void compareDdl(
      Connection conn1, Connection conn2, String sql1, String sql2, List<String> differences) {
    Object result1 = executeDdl(conn1, sql1);
    Object result2 = executeDdl(conn2, sql2);

    if (result1 instanceof Exception && result2 instanceof Exception) {
      // Both threw — compare exception types
      String type1 = result1.getClass().getSimpleName();
      String type2 = result2.getClass().getSimpleName();
      if (!type1.equals(type2)) {
        differences.add("Exception type mismatch: " + type1 + " vs " + type2);
      }
    } else if (result1 instanceof Exception) {
      differences.add("Thrift threw " + result1.getClass().getSimpleName() + " but SEA succeeded");
    } else if (result2 instanceof Exception) {
      differences.add("Thrift succeeded but SEA threw " + result2.getClass().getSimpleName());
    } else {
      // Both succeeded — compare return values
      if (!result1.equals(result2)) {
        differences.add("Return value mismatch: " + result1 + " (Thrift) vs " + result2 + " (SEA)");
      }
    }
  }

  /** Executes DDL and returns the int result or the Exception if it threw. */
  private Object executeDdl(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      return stmt.executeUpdate(sql);
    } catch (Exception e) {
      return e;
    }
  }

  /**
   * Verifies a side effect by running a query on each connection and checking that both return the
   * expected row count. Also compares that both return the same count.
   */
  private void verifySideEffect(
      Connection conn1,
      Connection conn2,
      String sql1,
      String sql2,
      int expectedRows,
      List<String> differences,
      String context) {
    int count1 = countRows(conn1, sql1);
    int count2 = countRows(conn2, sql2);
    if (count1 != expectedRows) {
      differences.add(
          "Thrift side-effect ("
              + context
              + "): expected "
              + expectedRows
              + " rows, got "
              + count1);
    }
    if (count2 != expectedRows) {
      differences.add(
          "SEA side-effect (" + context + "): expected " + expectedRows + " rows, got " + count2);
    }
    if (count1 != count2) {
      differences.add(
          "Row count mismatch (" + context + "): Thrift=" + count1 + " vs SEA=" + count2);
    }
  }

  private int countRows(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      int count = 0;
      while (rs.next()) count++;
      return count;
    } catch (Exception e) {
      return -1;
    }
  }

  /** Fire-and-forget DDL execution for setup/teardown. */
  private void exec(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (Exception ignored) {
    }
  }
}
