package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.Session;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

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
   * @param statement SQL statement that needs to be executed
   * @param warehouseId warehouse-Id which should be used for statement execution
   * @param parameters SQL parameters for the statement
   * @param statementType type of statement (metadata, update or generic SQL)
   * @param session underlying session
   * @return response for statement execution
   */
  DatabricksResultSet executeStatement(
      String statement, String warehouseId, Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType, IDatabricksSession session) throws SQLException;

  /**
   * Closes a statement in Databricks server
   * @param statementId statement which should be closed
   * @return response for statement execution
   */
  void closeStatement(String statementId);

  /**
   * Fetches the chunk details for given chunk index and statement-Id.
   * @param statementId statement-Id for which chunk should be fetched
   * @param chunkIndex chunkIndex for which chunk should be fetched
   */
  Collection<ExternalLink> getResultChunk(String statementId, long chunkIndex);
}
