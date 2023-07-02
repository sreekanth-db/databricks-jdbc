package com.databricks.sql.client.core;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.CreateSessionRequest;
import com.databricks.sdk.service.sql.Session;
import com.databricks.sdk.service.sql.StatementExecutionService;
import com.databricks.sql.client.jdbc.DatabricksConnectionContext;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * Implementation for Session interface, which maintains an underlying session in SQL Gateway.
 */
public class DatabricksSession implements IDatabricksSession {

  private final DatabricksConnectionContext connectionContext;

  private boolean isSessionOpen;
  private final DatabricksConfig databricksConfig;
  private final String warehouseId;

  private final WorkspaceClient workspaceClient;
  private Session session;

  /**
   * Creates an instance of Databricks session for given connection context
   * @param connectionContext underlying connection context
   */
  public DatabricksSession(DatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.isSessionOpen = false;
    this.session = null;
    this.databricksConfig = new DatabricksConfig();
    databricksConfig.setHost(connectionContext.getHostUrl());
    databricksConfig.setToken(connectionContext.getToken());

    this.warehouseId = connectionContext.getWarehouse();
    this.workspaceClient = new WorkspaceClient(databricksConfig);
  }

  /**
   * Construct method to be used for mocking in a test case.
   */
  @VisibleForTesting
  DatabricksSession(DatabricksConnectionContext connectionContext, StatementExecutionService statementExecutionService) {
    this.connectionContext = connectionContext;
    this.isSessionOpen = false;
    this.session = null;
    this.databricksConfig = new DatabricksConfig();
    this.warehouseId = connectionContext.getWarehouse();
    this.workspaceClient = new WorkspaceClient(true).withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  @Nullable
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
      // TODO: handle closed connections by server
      workspaceClient.statementExecution().deleteSession(this.session.getSessionId());
      this.session = null;
      this.isSessionOpen = false;
    }
  }
}
