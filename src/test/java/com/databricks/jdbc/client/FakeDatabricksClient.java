package com.databricks.jdbc.client;

import com.databricks.jdbc.core.IDatabricksResultSet;
import com.databricks.sdk.service.sql.*;

public class FakeDatabricksClient implements DatabricksClient {

  private final StatementExecutionService statementExecutionService;

  public FakeDatabricksClient(StatementExecutionService statementExecutionService) {
    this.statementExecutionService = statementExecutionService;
  }

  @Override
  public Session createSession(String warehouseId) {
    return statementExecutionService.createSession(
        new CreateSessionRequest().setSession(new Session().setWarehouseId(warehouseId)));
  }

  @Override
  public void deleteSession(String sessionId) {
    statementExecutionService.deleteSession(new DeleteSessionRequest().setSessionId(sessionId));
  }

  @Override
  public IDatabricksResultSet executeStatement(String statement, String sessionId, String warehouseId) {
    return null;
  }
}