package com.databricks.sql.client.core;

import com.databricks.sql.client.jdbc.DatabricksConnectionContext;

public class DatabricksSession implements IDatabricksSession {

  private final DatabricksConnectionContext connectionContext;

  private boolean isSessionOpen;
  private String sessionId;

  public DatabricksSession(DatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.isSessionOpen = false;
    this.sessionId = null;
  }
  @Override
  public String getSessionId() {
    return sessionId;
  }

  @Override
  public boolean isOpen() {
    return isSessionOpen;
  }

  @Override
  public void open() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
