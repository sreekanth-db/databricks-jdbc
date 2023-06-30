package com.databricks.jdbc.core;

/**
 * Interface providing Databricks specific Connection APIs.
 */
public interface IDatabricksConnection {

  /**
   * Returns the underlying session for the connection.
   */
  IDatabricksSession getSession();

  /**
   * Closes a statement from the connection's active set.
   * @param statement
   */
  void closeStatement(IDatabricksStatement statement);
}
