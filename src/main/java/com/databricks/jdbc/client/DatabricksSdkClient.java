package com.databricks.jdbc.client;

import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.CreateSessionRequest;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementResponse;
import com.databricks.sdk.service.sql.Session;

/**
 * Implementation of DatabricksClient interface using Databricks Java SDK.
 */
public class DatabricksSdkClient implements DatabricksClient {


  private final DatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  public DatabricksSdkClient(DatabricksConnectionContext connectionContext) {
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
  public ExecuteStatementResponse executeStatement(ExecuteStatementRequest request) {
    return null;
  }
}
