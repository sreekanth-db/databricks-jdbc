package com.databricks.jdbc.client.impl;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksResultSet;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;

/**
 * Implementation of DatabricksClient interface using Databricks Java SDK.
 */
public class DatabricksSdkClient implements DatabricksClient {

  private static final String ASYNC_TIMEOUT_VALUE = "0s";

  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig = new DatabricksConfig()
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(databricksConfig);
  }

  @Override
  public Session createSession(String warehouseId) {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest()
        .setWarehouseId(warehouseId);
    return workspaceClient.statementExecution().createSession(createSessionRequest);
  }

  @Override
  public void deleteSession(String sessionId) {
    workspaceClient.statementExecution().deleteSession(sessionId);
  }

  @Override
  public DatabricksResultSet executeStatement(String statement, String sessionId, String warehouseId) {
    // TODO: change disposition and format, and handle pending result
    ExecuteStatementRequest request = new ExecuteStatementRequest()
        .setStatement(statement)
        .setWarehouseId(warehouseId)
        .setDisposition(Disposition.EXTERNAL_LINKS)
        .setFormat(Format.ARROW_STREAM)
        .setWaitTimeout(ASYNC_TIMEOUT_VALUE)
        .setSessionId(sessionId);

    ExecuteStatementResponse response = workspaceClient.statementExecution().executeStatement(request);
    String statementId = response.getStatementId();
    StatementState responseState = response.getStatus().getState();
    if (responseState.equals(StatementState.SUCCEEDED)) {
      return new DatabricksResultSet(response.getStatus(), statementId, response.getResult(),
          response.getManifest());
    }
    while (responseState.equals(StatementState.PENDING) || responseState.equals(StatementState.RUNNING)) {
      GetStatementResponse getStatementResponse = workspaceClient.statementExecution().getStatement(statementId);
      responseState = getStatementResponse.getStatus().getState();
      if (responseState.equals(StatementState.SUCCEEDED)) {
        return new DatabricksResultSet(getStatementResponse.getStatus(), statementId, getStatementResponse.getResult(),
            getStatementResponse.getManifest());
      }

    }



  }

  @Override
  public void closeStatement(String statementId) {
    workspaceClient.statementExecution().closeStatement(statementId);
  }
}
