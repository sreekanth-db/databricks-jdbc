package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.core.*;
import com.databricks.sdk.service.sql.ExternalLink;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class DatabricksThriftClient implements DatabricksClient, DatabricksMetadataClient {
  @Override
  public ImmutableSessionInfo createSession(String warehouseId) {
    return null;
  }

  @Override
  public void deleteSession(String sessionId, String warehouseId) {}

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      String warehouseId,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException {
    return null;
  }

  @Override
  public void closeStatement(String statementId) {}

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    return null;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    return null;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) {
    return null;
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) {
    return null;
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern) {
    return null;
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    return null;
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern) {
    return null;
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern) {
    return null;
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) {
    return null;
  }
}
