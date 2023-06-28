package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksSdkClient;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.sdk.service.sql.CreateSessionRequest;
import com.databricks.sdk.service.sql.Session;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * Implementation for Session interface, which maintains an underlying session in SQL Gateway.
 */
public class DatabricksSession implements IDatabricksSession {

  private final DatabricksClient databricksClient;
  private final String warehouseId;

  private boolean isSessionOpen;
  private Session session;

  public DatabricksSession(DatabricksConnectionContext connectionContext) {
    this.databricksClient = new DatabricksSdkClient(connectionContext);
    this.isSessionOpen = false;
    this.session = null;
    this.warehouseId = parseWarehouse(connectionContext.getHttpPath());
  }

  @VisibleForTesting
  DatabricksSession(DatabricksConnectionContext connectionContext, DatabricksClient databricksClient) {
    this.databricksClient = databricksClient;
    this.isSessionOpen = false;
    this.session = null;
    this.warehouseId = parseWarehouse(connectionContext.getHttpPath());
  }

  private String parseWarehouse(String httpPath) {
    return httpPath.substring(httpPath.lastIndexOf('/') +1);
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
      this.session = databricksClient.createSession(this.warehouseId);
      this.isSessionOpen = true;
    }
  }

  @Override
  public void close() {
    // TODO: check for any pending query executions
    if (isSessionOpen) {
      // TODO: handle closed connections by server
      databricksClient.deleteSession(this.session.getSessionId());
      this.session = null;
      this.isSessionOpen = false;
    }
  }
}
