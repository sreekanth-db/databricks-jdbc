package com.databricks.jdbc.client;

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementResponse;
import com.databricks.sdk.service.sql.Session;

/**
 * Interface for Databricks client which abstracts the integration with Databricks server.
 */
public interface DatabricksClient {

  /**
   * Creates a new session for given warehouse-Id.
   * @param warehouseId for which a session should be created
   * @return created session
   */
  Session createSession(String warehouseId);

  /**
   * Deletes a session for given session-Id
   * @param sessionId for which the session should be deleted
   */
  void deleteSession(String sessionId);

  /**
   * Executes a statement in Databricks server
   * TODO: Use POJOs for request and response and remove dependency on SDK classes
   * @param request execute statement request
   * @return response for statement execution
   */
  ExecuteStatementResponse executeStatement(ExecuteStatementRequest request);
}