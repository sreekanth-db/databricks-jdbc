package com.databricks.jdbc.client.impl;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of DatabricksClient interface using Databricks Java SDK.
 */
public class DatabricksSdkClient implements DatabricksClient {

  private static final String ASYNC_TIMEOUT_VALUE = "0s";
  private static final String SYNC_TIMEOUT_VALUE = "20s";
  private static final int STATEMENT_RESULT_POLL_INTERVAL_MILLIS = 200;

  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig = new DatabricksConfig()
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(databricksConfig);
  }

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext, StatementExecutionService statementExecutionService) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig = new DatabricksConfig()
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(true).withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public Session createSession(String warehouseId) {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest()
        .setWarehouseId(warehouseId);
    return workspaceClient.statementExecution().createSession(createSessionRequest);
  }

  @Override
  public void deleteSession(String sessionId) {
    workspaceClient.statementExecution().deleteSession(sessionId);
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql, String warehouseId, Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType, IDatabricksSession session, IDatabricksStatement parentStatement) throws SQLException {

    Format format = useCloudFetchForResult(statementType) ? Format.ARROW_STREAM : Format.JSON_ARRAY;
    Disposition disposition = useCloudFetchForResult(statementType) ? Disposition.EXTERNAL_LINKS : Disposition.INLINE;
    ExecuteStatementRequest request = new ExecuteStatementRequest()
        .setStatement(sql)
        .setWarehouseId(warehouseId)
        .setDisposition(disposition)
        .setFormat(format)
        .setWaitTimeout(ASYNC_TIMEOUT_VALUE)
        .setSessionId(session.getSessionId());

    ExecuteStatementResponse response = workspaceClient.statementExecution().executeStatement(request);
    String statementId = response.getStatementId();
    StatementState responseState = response.getStatus().getState();

    // TODO: Add timeout
    while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
      try {
        // TODO: make this configurable
        Thread.sleep(STATEMENT_RESULT_POLL_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        // TODO: Handle gracefully
        throw new DatabricksSQLException("Statement execution fetch interrupted");
      }
      response = wrapGetStatementResponse(workspaceClient.statementExecution().getStatement(statementId));
      responseState = response.getStatus().getState();
    }
    if (responseState != StatementState.SUCCEEDED) {
      handleFailedExecution(responseState, statementId, sql);
    }

    return new DatabricksResultSet(response.getStatus(), statementId, response.getResult(),
          response.getManifest(), statementType, session, parentStatement);
  }

  private boolean useCloudFetchForResult(StatementType statementType) {
    return statementType == StatementType.QUERY || statementType == StatementType.SQL;
  }

  @Override
  public void closeStatement(String statementId) {
    workspaceClient.statementExecution().closeStatement(statementId);
  }

  @Override
  public Optional<ExternalLink> getResultChunk(String statementId, long chunkIndex) {
    return workspaceClient.statementExecution().getStatementResultChunkN(statementId, chunkIndex).getExternalLinks().stream().findFirst();
  }

  /**
   * Handles a failed execution and throws appropriate exception
   */
  private void handleFailedExecution(StatementState statementState, String statementId) throws SQLException {

    switch (statementState) {
      case FAILED:
      case CLOSED:
      case CANCELED:
        // TODO: Handle differently for failed, closed and cancelled with proper error codes
        throw new DatabricksSQLException("Statement execution failed " + statementId);
      default:
        throw new IllegalStateException("Invalid state for error");
    }
  }
  /**
   * Handles a failed execution and throws appropriate exception
   */
  private void handleFailedExecution(StatementState statementState, String statementId, String statement) throws SQLException {

    switch (statementState) {
      case FAILED:
      case CLOSED:
      case CANCELED:
        // TODO: Handle differently for failed, closed and cancelled with proper error codes
        throw new DatabricksSQLException("Statement execution failed " + statementId + " -> " + statement);
      default:
        throw new IllegalStateException("Invalid state for error");
    }
  }

  private ExecuteStatementResponse wrapGetStatementResponse(GetStatementResponse getStatementResponse) {
    return new ExecuteStatementResponse().setStatementId(getStatementResponse.getStatementId())
        .setStatus(getStatementResponse.getStatus())
        .setManifest(getStatementResponse.getManifest())
        .setResult(getStatementResponse.getResult());
  }

}
