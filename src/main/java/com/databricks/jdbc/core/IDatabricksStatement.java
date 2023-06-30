package com.databricks.jdbc.core;

/**
 * Interface for Databricks specific statement.
 */
public interface IDatabricksStatement {

  /**
   * Returns the underlying session-Id for the statement.
   */
  String getSessionId();

  void close(boolean removeFromSession);
}