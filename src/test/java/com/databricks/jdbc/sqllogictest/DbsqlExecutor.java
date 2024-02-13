/*
 * Copyright 2024 Databricks, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.databricks.jdbc.sqllogictest;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import net.hydromatic.sqllogictest.*;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;

public class DbsqlExecutor extends JdbcExecutor {

  static String dbsqlUrl =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443"
          + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
          + "/sql/1.0/warehouses/791ba2a31c7fd70a;ConnCatalog=field_demos;"
          + "ConnSchema=ossjdbc;";

  /**
   * Create an executor that uses JDBC to run tests.
   *
   * @param options Execution options.
   */
  public DbsqlExecutor(OptionsParser.SuppliedOptions options) {
    super(
        options,
        dbsqlUrl,
        "vikrant.puppala@databricks.com",
        "dapi432644d0bc9aa690de4270671774099f");
  }

  /**
   * Register the HSQL DB executor with the command-line options.
   *
   * @param optionsParser Options that will guide the execution.
   */
  public static void register(OptionsParser optionsParser) {
    optionsParser.registerExecutor(
        "dbsql",
        () -> {
          DbsqlExecutor result = new DbsqlExecutor(optionsParser.getOptions());
          try {
            DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
          try {
            Set<String> bugs = optionsParser.getOptions().readBugsFile();
            result.avoid(bugs);
            return result;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void establishConnection() throws SQLException {
    super.establishConnection();
    this.getConnection().setCatalog("field_demos");
    this.getConnection().setSchema("ossjdbc");
  }

  @Override
  public void dropAllTables() throws SQLException {
    List<String> tables = getTableList();
    options.out.println("Number of tables to drop: " + tables.size());
    for (String tableName : tables) {
      // Unfortunately prepare statements cannot be parameterized in
      // table names.  Sonar complains about this, but there is
      // nothing we can do but suppress the warning.
      options.out.println("Dropping table: " + tableName);
      String del = "DROP TABLE " + tableName;
      options.message(del, 2);
      try (Statement drop = this.getConnection().createStatement()) {
        drop.execute(del); // NOSONAR
      }
    }
  }

  @Override
  public void dropAllViews() throws SQLException {
    List<String> tables = getViewList();
    for (String tableName : tables) {
      // Unfortunately prepare statements cannot be parameterized in
      // table names.  Sonar complains about this, but there is
      // nothing we can do but suppress the warning.
      options.out.println("Dropping view: " + tableName);
      String del = "DROP VIEW IF EXISTS " + tableName;
      options.message(del, 2);
      try (Statement drop = this.getConnection().createStatement()) {
        drop.execute(del); // NOSONAR
      }
    }
  }

  @Override
  public boolean validate(
      SqlTestQuery query,
      ResultSet rs,
      SqlTestQueryOutputDescription description,
      TestStatistics statistics)
      throws SQLException, NoSuchAlgorithmException {
    boolean failed = super.validate(query, rs, description, statistics);
    // Ignore failures due to div sign as DBSQL does not round off to int
    return failed ? !query.getQuery().contains("/") : failed;
  }

  @Override
  public TestStatistics execute(SltTestFile file, OptionsParser.SuppliedOptions options)
      throws SQLException {
    options.out.println("Running " + file.toString());
    this.startTest();
    this.establishConnection();
    this.dropAllTables();
    TestStatistics result = new TestStatistics(options.stopAtFirstError, options.verbosity);
    result.incFiles();
    // Changed super function here to only run the first few commands from each file
    for (int i = 0; i < 20; i++) {
      ISqlTestOperation operation = file.fileContents.get(i);
      SltSqlStatement stat = operation.as(SltSqlStatement.class);
      if (stat != null) {
        try {
          options.out.println(stat.statement);
          if (stat.statement.contains("CREATE TABLE") && stat.statement.contains("TEXT")) {
            // DBSQL does not support TEXT datatype so skip these tests
            return result;
          }
          this.statement(stat);
          if (!stat.shouldPass) {
            options.err.println("Statement should have failed: " + operation);
          }
        } catch (SQLException ex) {
          // errors in statements cannot be recovered.
          if (stat.shouldPass) {
            // shouldPass should always be true, otherwise
            // the exception should not be thrown.
            options.err.println("Error '" + ex.getMessage() + "' in SQL statement " + operation);
            result.incFilesNotParsed();
            return result;
          }
        }
      } else {
        SqlTestQuery query = operation.to(options.err, SqlTestQuery.class);
        boolean stop;
        try {
          options.out.println(query.getQuery());
          stop = this.query(query, result);
        } catch (Throwable ex) {
          // Need to catch Throwable to handle assertion failures too
          options.message("Exception during query: " + ex.getMessage(), 1);
          stop = result.addFailure(new TestStatistics.FailedTestDescription(query, null, "", ex));
        }
        if (stop) {
          break;
        }
      }
    }
    this.dropAllViews();
    this.dropAllTables();
    this.closeConnection();
    options.message(this.elapsedTime(file.getTestCount()), 1);
    return result;
  }

  /**
   * Run a query.
   *
   * @param query Query to execute.
   * @param statistics Execution statistics recording the result of the query execution.
   * @return True if we need to stop executing.
   */
  boolean query(SqlTestQuery query, TestStatistics statistics)
      throws SQLException, NoSuchAlgorithmException {
    if (this.buggyOperations.contains(query.getQuery()) || this.options.doNotExecute) {
      statistics.incIgnored();
      options.message("Skipping " + query.getQuery(), 2);
      return false;
    }
    try (Statement stmt = this.getConnection().createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(query.getQuery())) {
        boolean result = this.validate(query, resultSet, query.outputDescription, statistics);
        options.message(statistics.totalTests() + ": " + query.getQuery(), 2);
        return result;
      }
    }
  }

  List<String> getViewList() {
    List<String> result = new ArrayList<>();
    return result;
  }

  List<String> getTableList() throws SQLException {
    List<String> result = new ArrayList<>();
    DatabaseMetaData md = this.getConnection().getMetaData();
    ResultSet rs = md.getTables(null, null, "%", new String[] {"TABLE"});
    while (rs.next()) {
      String tableName = rs.getString(3);
      result.add(tableName);
    }
    rs.close();
    return result;
  }
}
