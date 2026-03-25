package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Compares DatabaseMetaData method results between Thrift and SEA.
 *
 * <p>Each test case corresponds to one DatabaseMetaData method. The execute() method iterates all
 * argument combinations for that method, invokes via reflection on both connections' metadata
 * objects, and compares results using ResultSetComparator.
 *
 * <p>The test plan (which methods, which args) is defined in {@link DatabaseMetaDataParams}. This
 * class is purely about execution and comparison.
 */
public class DatabaseMetaDataProvider implements SuiteProvider {

  private static final Map<String, List<Object[]>> METHOD_REGISTRY =
      DatabaseMetaDataParams.buildRegistry();

  /** Schemas to exclude from ResultSet comparison. Set via -DMETADATA_SKIP_SCHEMAS. */
  private static final Set<String> SKIP_SCHEMAS = parseSkipSchemas();

  private static Set<String> parseSkipSchemas() {
    String prop = System.getProperty("METADATA_SKIP_SCHEMAS");
    if (prop == null || prop.isEmpty()) return Set.of();
    return new HashSet<>(Arrays.asList(prop.split(",")));
  }

  @Override
  public List<TestCase> getTestCases() {
    List<TestCase> cases = new ArrayList<>();
    for (Map.Entry<String, List<Object[]>> entry : METHOD_REGISTRY.entrySet()) {
      String method = entry.getKey();
      int comboCount = entry.getValue().size();
      cases.add(new TestCase(method, method + " — " + comboCount + " arg combo(s)"));
    }
    return cases;
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String methodName = testCase.getIdentifier();
    List<Object[]> argCombos = METHOD_REGISTRY.get(methodName);

    DatabaseMetaData md1 = conn1.getMetaData();
    DatabaseMetaData md2 = conn2.getMetaData();

    List<String> metadataDiffs = new ArrayList<>();
    List<String> dataDiffs = new ArrayList<>();

    int total = argCombos.size();
    for (int idx = 0; idx < total; idx++) {
      Object[] args = argCombos.get(idx);
      String argsLabel = formatArgs(args);
      System.out.printf(
          "[%s]   Comparing %s(%s) [%d/%d]%n",
          Instant.now(), methodName, argsLabel, idx + 1, total);

      Object result1 = ReflectionUtils.executeMethod(md1, methodName, args);
      Object result2 = ReflectionUtils.executeMethod(md2, methodName, args);

      try {
        ComparisonResult sub;
        if (!SKIP_SCHEMAS.isEmpty()
            && result1 instanceof ResultSet
            && result2 instanceof ResultSet) {
          sub =
              compareWithSchemaFilter(
                  label, methodName, args, (ResultSet) result1, (ResultSet) result2);
        } else {
          sub = ResultSetComparator.compare(label, methodName, args, result1, result2);
        }

        // Prefix each diff with the args for traceability
        for (String diff : sub.metadataDifferences) {
          metadataDiffs.add("[" + argsLabel + "] " + diff);
        }
        for (String diff : sub.dataDifferences) {
          dataDiffs.add("[" + argsLabel + "] " + diff);
        }
      } finally {
        // Close ResultSets to avoid resource leaks
        if (result1 instanceof ResultSet) ((ResultSet) result1).close();
        if (result2 instanceof ResultSet) ((ResultSet) result2).close();
      }
    }

    ComparisonResult result = new ComparisonResult(label, methodName, new Object[0]);
    result.metadataDifferences = metadataDiffs;
    result.dataDifferences = dataDiffs;
    return result;
  }

  /**
   * Drains both ResultSets into row lists, filters out rows matching SKIP_SCHEMAS, then compares.
   */
  private static ComparisonResult compareWithSchemaFilter(
      String label, String methodName, Object[] args, ResultSet rs1, ResultSet rs2)
      throws SQLException {
    ResultSetMetaData rsMd1 = rs1.getMetaData();
    ResultSetMetaData rsMd2 = rs2.getMetaData();

    List<Object[]> rows1 = drainResultSet(rs1, rsMd1.getColumnCount());
    List<Object[]> rows2 = drainResultSet(rs2, rsMd2.getColumnCount());

    filterBySchema(rows1, rsMd1);
    filterBySchema(rows2, rsMd2);

    return ResultSetComparator.compareRows(label, methodName, args, rsMd1, rows1, rsMd2, rows2);
  }

  private static List<Object[]> drainResultSet(ResultSet rs, int columnCount) throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    while (rs.next()) {
      Object[] row = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        row[i] = rs.getObject(i + 1);
      }
      rows.add(row);
    }
    return rows;
  }

  private static void filterBySchema(List<Object[]> rows, ResultSetMetaData md)
      throws SQLException {
    int schemaCol = findColumnIndex(md, "TABLE_SCHEM");
    if (schemaCol < 0) return;
    rows.removeIf(
        row -> row[schemaCol] != null && SKIP_SCHEMAS.contains(row[schemaCol].toString()));
  }

  private static int findColumnIndex(ResultSetMetaData md, String columnName) throws SQLException {
    for (int i = 1; i <= md.getColumnCount(); i++) {
      if (columnName.equalsIgnoreCase(md.getColumnName(i))) {
        return i - 1; // 0-based for array access
      }
    }
    return -1;
  }

  private static String formatArgs(Object[] args) {
    if (args.length == 0) return "no args";
    return Arrays.stream(args)
        .map(
            o -> {
              if (o == null) return "null";
              if (o instanceof Object[]) return Arrays.toString((Object[]) o);
              return o.toString();
            })
        .collect(Collectors.joining(", "));
  }
}
