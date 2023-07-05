package com.databricks.jdbc.client;

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
    this.databricksConfig = new DatabricksConfig();
    databricksConfig.setHost(connectionContext.getHostUrl());
    databricksConfig.setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(databricksConfig);
  }

  @Override
  public Session createSession(String warehouseId) {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest()
        .setSession(new Session().setWarehouseId(warehouseId));
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
        .setDisposition(Disposition.INLINE)
        .setFormat(Format.JSON_ARRAY)
        .setWaitTimeout(ASYNC_TIMEOUT_VALUE)
        .setSessionId(sessionId);
    ExecuteStatementResponse response = workspaceClient.statementExecution().executeStatement(request);

    return new DatabricksResultSet(response.getStatus(), response.getStatementId(), response.getResult(),
        response.getManifest());
  }

  @Override
  public void closeStatement(String statementId) {
    workspaceClient.statementExecution().closeStatement(statementId);
  }
}
