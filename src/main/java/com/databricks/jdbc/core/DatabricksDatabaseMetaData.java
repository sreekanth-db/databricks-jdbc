package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;

public class DatabricksDatabaseMetaData implements DatabaseMetaData {

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
    return DatabricksJdbcConstants.PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.DATABASE_MAJOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.DATABASE_MINOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.DATABASE_PATCH_VERSION;
  }

  @Override
  public String getDriverName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.JDBC_MAJOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.JDBC_MINOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.JDBC_PATCH_VERSION;
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
    return DatabricksJdbcConstants.NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.STRING_FUNCTIONS;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.SYSTEM_FUNCTIONS;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.TIME_DATE_FUNCTIONS;
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
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throwExceptionIfConnectionIsClosed();
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
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
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
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
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
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    // TODO: Handle pattern for schema, assuming schema is not a regex for now

    if (catalog == null) {
      catalog = session.getCatalog();
    }
    if (catalog == null) {
      // TODO: return an empty result set
    }
    if (schemaPattern == null) {
      schemaPattern = session.getSchema();
    }
    if (schemaPattern == null) {
      // TODO: return an empty result set
    }

    String showTablesSQL = "show tables from " + catalog + "." + schemaPattern + " like '" + tableNamePattern + "'";
    return session.getDatabricksClient().executeStatement(showTablesSQL, session.getSessionId(), session.getWarehouseId(), true);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null /* catalog */, null /* schema pattern */);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throwExceptionIfConnectionIsClosed();

    String showCatalogsSQL = "show catalogs";

    return session.getDatabricksClient().executeStatement(showCatalogsSQL, session.getSessionId(),
            session.getWarehouseId(), true);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(new StatementStatus().setState(StatementState.SUCCEEDED),
            "metadata-statement",
            Collections.singletonList("TABLE_TYPE"),
            Collections.singletonList("TEXT"),
            Collections.singletonList(ColumnInfoTypeName.STRING),
            Collections.singletonList(255),
            new String[][] {{"SYSTEM TABLE"}, {"TABLE"}, {"VIEW"}});
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    // TODO: Handle null catalog, schema, table behaviour

    String showSchemaSQL = "show columns in " + catalog + "." + schemaPattern + "." + tableNamePattern;
    ResultSet resultSet = session.getDatabricksClient().executeStatement(showSchemaSQL, session.getSessionId(), session.getWarehouseId(), true);

    // TODO: Handle post result set generation filtering based on result set implementation

    return resultSet;
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
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
    throw new UnsupportedOperationException("Not implemented");
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
    return DatabricksJdbcConstants.DATABASE_MAJOR_VERSION;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return DatabricksJdbcConstants.DATABASE_MINOR_VERSION;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return DatabricksJdbcConstants.JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return DatabricksJdbcConstants.JDBC_MINOR_VERSION;
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

    if (catalog == null) {
        catalog = session.getCatalog();
    }

    if (catalog == null) {
      // TODO: Return an empty result set
    }

    String showSchemaSQL = "show schemas in " + catalog + " like \'" + schemaPattern + "\'";
    return session.getDatabricksClient().executeStatement(showSchemaSQL, session.getSessionId(), session.getWarehouseId(), true);
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

    // TODO: Handle null catalog, schema, function behaviour

    String showSchemaSQL = "show functions in " + catalog + "." + schemaPattern + " like '" + functionNamePattern + "'";
    return session.getDatabricksClient().executeStatement(showSchemaSQL, session.getSessionId(), session.getWarehouseId(), true);
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
}
