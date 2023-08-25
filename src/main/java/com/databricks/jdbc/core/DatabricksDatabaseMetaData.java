package com.databricks.jdbc.core;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.util.WildcardUtil;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabricksDatabaseMetaData implements DatabaseMetaData {
  public static final String DRIVER_NAME = "DatabricksJDBC";
  public static final String PRODUCT_NAME = "SparkSQL";
  public static final int DATABASE_MAJOR_VERSION = 3;
  public static final int DATABASE_MINOR_VERSION = 1;
  public static final int DATABASE_PATCH_VERSION = 1;
  public static final int JDBC_MAJOR_VERSION = 0;
  public static final int JDBC_MINOR_VERSION = 0;
  public static final int JDBC_PATCH_VERSION = 0;
  public static final Integer MAX_NAME_LENGTH = 128;
  public static final String NUMERIC_FUNCTIONS = "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE";
  public static final String STRING_FUNCTIONS = "ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,INSERT,LCASE,LEFT,LENGTH,LOCATE,LOCATE2,LTRIM,OCTET_LENGTH,POSITION,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,UCASE";
  public static final String SYSTEM_FUNCTIONS = "DATABASE,IFNULL,USER";
  public static final String TIME_DATE_FUNCTIONS = "CURDATE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR";

  private final IDatabricksConnection connection;

  private final IDatabricksSession session;

  public DatabricksDatabaseMetaData(IDatabricksConnection connection) {
      this.connection = connection;
      this.session = connection.getSession();
  }
  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getUserName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.USER_NAME;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DATABASE_MAJOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DATABASE_MINOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DATABASE_PATCH_VERSION;
  }

  @Override
  public String getDriverName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return JDBC_MAJOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + JDBC_MINOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + JDBC_PATCH_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getDriverMinorVersion() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.IDENTIFIER_QUOTE_STRING;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return STRING_FUNCTIONS;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return SYSTEM_FUNCTIONS;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return TIME_DATE_FUNCTIONS;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.SCHEMA;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.PROCEDURE;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.CATALOG;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.FULL_STOP;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    // Open cursors are not supported, however open statements are.
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    // Open cursors are not supported, however open statements are.
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    // TODO: check once, simba returns empty result set as well
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            "getprocedures-metadata",
            Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "NUM_INPUT_PARAMS", "NUM_OUTPUT_PARAMS", "NUM_RESULT_SETS", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
            Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
            Arrays.asList(128, 128, 128, 128, 128, 128, 128, 128, 128),
            new Object[0][0],
            StatementType.METADATA
    );
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    Map.Entry<String, String> pair = applyContext(catalog, schemaPattern);
    String catalogWithContext = pair.getKey();
    String schemaWithContext = pair.getValue();
    Queue<Map.Entry<String, String>> catalogSchemaPairs = new ConcurrentLinkedQueue<>();
    if (WildcardUtil.isWildcard(schemaWithContext) || WildcardUtil.isMatchAnything(catalogWithContext)) {
      ResultSet resultSet = getSchemas(catalogWithContext, schemaWithContext);
      while (resultSet.next()) {
        catalogSchemaPairs.add(Map.entry(resultSet.getString(2), resultSet.getString(1)));
      }
    } else {
      catalogSchemaPairs.add(pair);
    }
    // TODO: Limit to 15 pairs to run quickly, remove after demo/find workaround
    while (catalogSchemaPairs.size() > 15)
      catalogSchemaPairs.poll();
    String tableWithContext = tableNamePattern == null ? "*" : tableNamePattern;

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(150);
    for (int i = 0; i < 150; i++) {
      executorService.submit(() -> {
        while (!catalogSchemaPairs.isEmpty()) {
          Map.Entry<String, String> currentPair = catalogSchemaPairs.poll();
          String currentCatalog = currentPair.getKey();
          String currentSchema = currentPair.getValue();
          String showTablesSQL = "show tables from " + currentCatalog + "." + currentSchema;
          if (!WildcardUtil.isMatchAnything(tableWithContext)) {
            showTablesSQL += " like '" + tableWithContext + "'";
          }
          try {
            ResultSet rs = session.getDatabricksClient().executeStatement(
                    showTablesSQL, session.getWarehouseId(), new HashMap<Integer, ImmutableSqlParameter>(),
                StatementType.METADATA, session, null);
            while (rs.next()) {
              rows.add(Arrays.asList(currentCatalog, currentSchema, rs.getString(2), "TABLE", null, null, null, null, null, null));
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      // wait
    }
    // They are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME.
    rows.sort(Comparator.comparing((List<Object> i) -> i.get(3).toString())
            .thenComparing(i -> i.get(0).toString()).thenComparing(i -> i.get(1).toString())
            .thenComparing(i -> i.get(2).toString()));
    return new DatabricksResultSet(new StatementStatus().setState(StatementState.SUCCEEDED),
            "gettables-metadata",
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
            Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
            Arrays.asList(128, 128, 128, 128, 128, 128, 128, 128, 128, 128),
            rows,
            StatementType.METADATA);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null /* catalog */, null /* schema pattern */);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throwExceptionIfConnectionIsClosed();

    String showCatalogsSQL = "show catalogs";

    ResultSet rs = session.getDatabricksClient().executeStatement(
            showCatalogsSQL, session.getWarehouseId(), new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA, session, null);
    List<List<Object>> rows = new ArrayList<>();
    while (rs.next()) {
      rows.add(Collections.singletonList(rs.getString(1)));
    }
    return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            "getcatalogs-metadata",
            Collections.singletonList("TABLE_CAT"),
            Collections.singletonList("VARCHAR"),
            Collections.singletonList(Types.VARCHAR),
            Collections.singletonList(128),
            rows,
            StatementType.METADATA);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(new StatementStatus().setState(StatementState.SUCCEEDED),
            "tabletype-metadata",
            Collections.singletonList("TABLE_TYPE"),
            Collections.singletonList("VARCHAR"),
            Collections.singletonList(Types.VARCHAR),
            Collections.singletonList(128),
            new String[][] {{"SYSTEM TABLE"}, {"TABLE"}, {"VIEW"}},
            StatementType.METADATA);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    ResultSet resultSet = getTables(catalog, schemaPattern, tableNamePattern, null);
    Queue<String[]> catalogSchemaTableCombinations = new ConcurrentLinkedQueue<>();
    while (resultSet.next()) {
      catalogSchemaTableCombinations.add(new String[]{resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)});
    }

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(150);
    for (int i = 0; i < 150; i++) {
      executorService.submit(() -> {
        while (!catalogSchemaTableCombinations.isEmpty()) {
          String[] combination = catalogSchemaTableCombinations.poll();
          String showSchemaSQL = "show columns in " + combination[0] + "." + combination[1] + "." + combination[2];
          try {
            ResultSet rs = session.getDatabricksClient().executeStatement(showSchemaSQL, session.getWarehouseId(),
                    new HashMap<Integer, ImmutableSqlParameter>(), StatementType.METADATA, session, null);
            while (rs.next()) {
              if (rs.getString(1).matches(columnNamePattern)) {
                rows.add(Arrays.asList(combination[0], combination[1], combination[2], rs.getString(1)));
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      // wait
    }
    // TODO: some columns are missing from result set, determine how to fill those
    rows.sort(Comparator.comparing((List<Object> i) -> i.get(0).toString())
            .thenComparing(i -> i.get(1).toString()).thenComparing(i -> i.get(2).toString()));
    return new DatabricksResultSet(new StatementStatus().setState(StatementState.SUCCEEDED),
            "metadata-statement",
            Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"),
            Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
            Arrays.asList(128, 128, 128, 128),
            rows,
            StatementType.METADATA);
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throwExceptionIfConnectionIsClosed();

    return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            "typeinfo-metadata",
            Arrays.asList(
                    "TYPE_NAME",
                    "DATA_TYPE",
                    "PRECISION",
                    "LITERAL_PREFIX",
                    "LITERAL_SUFFIX",
                    "CREATE_PARAMS",
                    "NULLABLE",
                    "CASE_SENSITIVE",
                    "SEARCHABLE",
                    "UNSIGNED_ATTRIBUTE",
                    "FIXED_PREC_SCALE",
                    "AUTO_INCREMENT",
                    "LOCAL_TYPE_NAME",
                    "MINIMUM_SCALE",
                    "MAXIMUM_SCALE",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "NUM_PREC_RADIX"),
            Arrays.asList(
                    "VARCHAR", "INTEGER", "INTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "SMALLINT", "BIT", "SMALLINT",
                    "BIT", "BIT", "BIT", "VARCHAR", "SMALLINT", "SMALLINT", "INTEGER", "INTEGER",
                    "INTEGER"),
            Arrays.asList(
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.BOOLEAN,
                    Types.SMALLINT,
                    Types.BOOLEAN,
                    Types.BOOLEAN,
                    Types.BOOLEAN,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER),
            Arrays.asList(
                    128,
                    10,
                    10,
                    128,
                    128,
                    128,
                    5,
                    1,
                    5,
                    1,
                    1,
                    1,
                    128,
                    5,
                    5,
                    10,
                    10,
                    10),
            new Object[][]{
                    {
                            "TINYINT",
                            Types.TINYINT,
                            3,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "TINYINT",
                            0,
                            0,
                            Types.TINYINT,
                            null,
                            10
                    },
                    {
                            "BIGINT",
                            Types.BIGINT,
                            19,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "BIGINT",
                            0,
                            0,
                            Types.BIGINT,
                            null,
                            10
                    },
                    {
                            "BINARY",
                            Types.BINARY,
                            32767,
                            "0x",
                            null,
                            "LENGTH",
                            typeNullable,
                            false,
                            typePredNone,
                            null,
                            false,
                            null,
                            "BINARY",
                            null,
                            null,
                            Types.BINARY,
                            null,
                            null
                    },
                    {
                            "CHAR",
                            Types.CHAR,
                            255,
                            "'",
                            "'",
                            "LENGTH",
                            typeNullable,
                            true,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "CHAR",
                            null,
                            null,
                            Types.CHAR,
                            null,
                            null
                    },
                    {
                            "DECIMAL",
                            Types.DECIMAL,
                            38,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "DECIMAL",
                            0,
                            0,
                            Types.DECIMAL,
                            null,
                            10
                    },
                    {
                            "INT",
                            Types.INTEGER,
                            10,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "INT",
                            0,
                            0,
                            Types.INTEGER,
                            null,
                            10
                    },
                    {
                            "SMALLINT",
                            Types.SMALLINT,
                            5,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "SMALLINT",
                            0,
                            0,
                            Types.SMALLINT,
                            null,
                            10
                    },
                    {
                            "FLOAT",
                            Types.FLOAT,
                            7,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "FLOAT",
                            null,
                            null,
                            Types.FLOAT,
                            null,
                            2
                    },
                    {
                            "DOUBLE",
                            Types.DOUBLE,
                            15,
                            null,
                            null,
                            null,
                            typeNullable,
                            false,
                            typePredBasic,
                            false,
                            false,
                            null,
                            "DOUBLE",
                            null,
                            null,
                            Types.DOUBLE,
                            null,
                            2
                    },
                    {
                            "ARRAY",
                            Types.VARCHAR,
                            32767,
                            "'",
                            "'",
                            "Type",
                            typeNullable,
                            false,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "ARRAY",
                            null,
                            null,
                            Types.VARCHAR,
                            null,
                            null
                    },
                    {
                            "MAP",
                            Types.VARCHAR,
                            32767,
                            "'",
                            "'",
                            "Key,Value",
                            typeNullable,
                            false,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "MAP",
                            null,
                            null,
                            Types.VARCHAR,
                            null,
                            null
                    },
                    {
                            "STRING",
                            Types.VARCHAR,
                            510,
                            "'",
                            "'",
                            "max length",
                            typeNullable,
                            true,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "STRING",
                            null,
                            null,
                            Types.VARCHAR,
                            null,
                            null
                    },
                    {
                            "STRUCT",
                            Types.VARCHAR,
                            32767,
                            "'",
                            "'",
                            "Column Type, ...",
                            typeNullable,
                            false,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "STRUCT",
                            null,
                            null,
                            Types.VARCHAR,
                            null,
                            null
                    },
                    {
                            "VARCHAR",
                            Types.VARCHAR,
                            510,
                            "'",
                            "'",
                            "max length",
                            typeNullable,
                            true,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "VARCHAR",
                            null,
                            null,
                            Types.VARCHAR,
                            null,
                            null
                    },
                    {
                            "BOOLEAN",
                            Types.BOOLEAN,
                            1,
                            null,
                            null,
                            null,
                            typeNullable,
                            true,
                            typePredBasic,
                            null,
                            false,
                            null,
                            "BOOLEAN",
                            null,
                            null,
                            Types.BOOLEAN,
                            null,
                            null
                    },
                    {
                            "DATE",
                            Types.DATE,
                            10,
                            "'",
                            "'",
                            null,
                            typeNullable,
                            false,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "DATE",
                            null,
                            null,
                            Types.DATE,
                            null,
                            null
                    },
                    {
                            "TIMESTAMP",
                            Types.TIMESTAMP,
                            29,
                            "'",
                            "'",
                            null,
                            typeNullable,
                            false,
                            typeSearchable,
                            null,
                            false,
                            null,
                            "TIMESTAMP",
                            0,
                            0,
                            Types.DATE,
                            null,
                            null
                    }
            },
            StatementType.METADATA);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    // TODO: implement, returning only empty set for now
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            "getudts-metadata",
            Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMAKRS", "BASE_TYPE"),
            Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
            Arrays.asList(128, 128, 128, 128, 128, 128, 128),
            new String[0][0],
            StatementType.METADATA
    );
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection.getConnection();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return DATABASE_MAJOR_VERSION;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return DATABASE_MINOR_VERSION;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return JDBC_MINOR_VERSION;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    Map.Entry<String, String> pair = applyContext(catalog, schemaPattern);
    String catalogWithContext = pair.getKey();
    String schemaWithContext = pair.getValue();

    // Since catalog must be an identifier or all catalogs (null), we need not care about catalog regex
    Queue<String> catalogs = new ConcurrentLinkedQueue<>();
    if (WildcardUtil.isMatchAnything(catalogWithContext)) {
      ResultSet rs = getCatalogs();
      while (rs.next()) {
        catalogs.add(rs.getString(1));
      }
    } else {
      catalogs.add(catalogWithContext);
    }
    // TODO: Remove post demo
    while (catalogs.size() > 5)
      catalogs.poll();

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(150);
    for (int i = 0; i < 150; i++) {
      executorService.submit(() -> {
        while (!catalogs.isEmpty()) {
          String currentCatalog = catalogs.poll();
          // TODO: Emoji characters are not being handled correctly by SDK/SEA, hence, skipping for now
          if (WildcardUtil.containsEmoji(currentCatalog))
            return;
          String showSchemaSQL = "show schemas in `" + currentCatalog + "`";
          if (!WildcardUtil.isMatchAnything(schemaWithContext)) {
            showSchemaSQL += " like '" + schemaWithContext + "'";
          }
          try {
            ResultSet rs = session.getDatabricksClient().executeStatement(
                    showSchemaSQL, session.getWarehouseId(), new HashMap<Integer, ImmutableSqlParameter>(),
                    StatementType.METADATA, session);
            while (rs.next()) {
              rows.add(Arrays.asList(rs.getString(1), currentCatalog));
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      // wait
    }
    rows.sort(Comparator.comparing((List<Object> i) -> i.get(1).toString()).thenComparing(i -> i.get(0).toString()));
    return new DatabricksResultSet(new StatementStatus().setState(StatementState.SUCCEEDED),
            "metadata-statement",
            Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
            Arrays.asList("VARCHAR", "VARCHAR"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR),
            Arrays.asList(128, 128),
            rows,
            StatementType.METADATA);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    // TODO: implement, returning only empty set for now
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            "getfunctions-metadata",
            Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
            Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
            Arrays.asList(128, 128, 128, 128, 128, 128),
            new Object[0][0],
            StatementType.METADATA
    );

//    // TODO: Handle null catalog, schema, function behaviour
//
//    String showSchemaSQL = "show functions in " + catalog + "." + schemaPattern + " like '" + functionNamePattern + "'";
//    return session.getDatabricksClient().executeStatement(showSchemaSQL, session.getWarehouseId(),
//        new HashMap<Integer, ImmutableSqlParameter>(), StatementType.METADATA, session);
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    if (!connection.getSession().isOpen()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }

  private Map.Entry<String, String> applyContext(String catalog, String schema) throws SQLException {
    if (catalog == null) {
      catalog = connection.getConnection().getCatalog();
    }
    // If catalog is not set in context, look at all catalogs
    if (catalog == null) {
      catalog = "*";
    }
    if (schema == null) {
      schema = connection.getConnection().getSchema();
    }
    // If schema is not set in context, look at all schemas
    if (schema == null) {
      schema = "*";
    }
    return Map.entry(catalog, schema);
  }
}
