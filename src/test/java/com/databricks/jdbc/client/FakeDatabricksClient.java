package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.sdk.service.sql.*;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public class FakeDatabricksClient implements DatabricksClient {

  private final StatementExecutionService statementExecutionService;

  public FakeDatabricksClient(StatementExecutionService statementExecutionService) {
    this.statementExecutionService = statementExecutionService;
  }

  @Override
  public Session createSession(String warehouseId) {
    return statementExecutionService.createSession(
        new CreateSessionRequest().setWarehouseId(warehouseId));
  }

  @Override
  public void deleteSession(String sessionId) {
    statementExecutionService.deleteSession(new DeleteSessionRequest().setSessionId(sessionId));
  }

  @Override
  public DatabricksResultSet executeStatement(
      String statement, String warehouseId, Map<Integer, ImmutableSqlParameter> params, StatementType statementType,
      IDatabricksSession session) throws SQLException {
    return null;
  }

  @Override
  public void closeStatement(String statementId) {
    statementExecutionService.closeStatement(new CloseStatementRequest().setStatementId(statementId));
  }

  @Override
  public Optional<ExternalLink> getResultChunk(String statementId, long chunkIndex) {
    return Optional.empty();
  }
}