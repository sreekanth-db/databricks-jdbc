package com.databricks.sql.client.core;

/**
 * Session interface to represent an open connection to Databricks server.
 */
public interface IDatabricksSession {

  /**
   * Get the unique session-Id associated with the session.
   * @return session-Id
   */
  String getSessionId();

  /**
   * Checks if session is open and valid.
   * @return true if session is open
   */
  boolean isOpen();

  /**
   * Opens a new session.
   */
  void open();

  /**
   * Closes the session.
   */
  void close();
}
