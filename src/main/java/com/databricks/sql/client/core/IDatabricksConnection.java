package com.databricks.sql.client.core;

/**
 * Interface providing Databricks specific Connection APIs.
 */
public interface IDatabricksConnection {

  /**
   * Returns the underlying session for the connection.
   */
  IDatabricksSession getSession();
}
