package com.databricks.jdbc.client;

public class DatabricksHttpException extends Exception {

  private Throwable cause;

  public DatabricksHttpException(String message) {
    super(message);
    this.cause = null;
  }

  public DatabricksHttpException(String message, Throwable cause) {
    super(message);
    this.cause = cause;
  }
}
