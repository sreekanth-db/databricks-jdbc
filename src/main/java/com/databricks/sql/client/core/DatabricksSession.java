package com.databricks.sql.client.core;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.CreateSessionRequest;
import com.databricks.sdk.service.sql.Session;
import com.databricks.sql.client.jdbc.DatabricksConnectionContext;
import com.databricks.sql.client.jdbc.DatabricksJdbcConstants;

public class DatabricksSession implements IDatabricksSession {

  private final DatabricksConnectionContext connectionContext;

  private boolean isSessionOpen;
  private DatabricksConfig databricksConfig;
  private String warehouseId;
  private WorkspaceClient workspaceClient;
  private Session session;

  public DatabricksSession(DatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.isSessionOpen = false;
    this.session = null;
  }

  private void initSessionConfigs() {
    this.databricksConfig = new DatabricksConfig();
    databricksConfig.setHost(connectionContext.getHostUrl());
    databricksConfig.setToken(connectionContext.getParameter(DatabricksJdbcConstants.TOKEN));

    this.warehouseId = parseWarehouse(connectionContext.getParameter(DatabricksJdbcConstants.HTTP_PATH));
    this.workspaceClient = new WorkspaceClient(databricksConfig);
  }

  private String parseWarehouse(String httpPath) {
    return httpPath.substring(httpPath.lastIndexOf('/') +1);
  }

  @Override
  public String getSessionId() {
    return isSessionOpen ? session.getSessionId() : null;
  }

  @Override
  public boolean isOpen() {
    return isSessionOpen;
  }

  @Override
  public void open() {
    // TODO: check for expired sessions
    if (!isSessionOpen) {
      CreateSessionRequest createSessionRequest = new CreateSessionRequest()
          .setSession(new Session().setWarehouseId(this.warehouseId));
      // TODO: handle errors
      this.session = workspaceClient.statementExecution().createSession(createSessionRequest);
      this.isSessionOpen = true;
    }
  }

  @Override
  public void close() {
    // TODO: check for any pending query executions
    if (isSessionOpen) {
      workspaceClient.statementExecution().deleteSession(this.session.getSessionId());
      this.session = null;
      this.isSessionOpen = false;
    }
  }
}
