package com.databricks.jdbc.core;

import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_SLEEP_DELAY;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.core.VolumeOperationExecutor.VolumeOperationStatus;
import com.databricks.sdk.service.sql.ResultSchema;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;

/** Class to handle the result of a volume operation */
class VolumeOperationResult implements IExecutionResult {

  private final IDatabricksSession session;
  private final ResultManifest manifest;
  private final String statementId;
  private VolumeOperationExecutor volumeOperationExecutor;
  private int currentRowIndex;

  VolumeOperationResult(
          String statementId, IDatabricksSession session, IExecutionResult resultHandler) {
    this.statementId = statementId;
    this.session = session;
    init(resultHandler,
        DatabricksHttpClient.getInstance(session.getConnectionContext()));
  }

  @VisibleForTesting
  VolumeOperationResult(
      String statementId,
      IDatabricksSession session,
      IExecutionResult resultHandler,
      IDatabricksHttpClient httpClient) {
    this.statementId = statementId;
    this.session = session;
    this.currentRowIndex = -1;
    init(resultHandler, httpClient);
  }

  private void init(IExecutionResult resultHandler, IDatabricksHttpClient httpClient) {
    // For now there would be only one external link, until multi part upload is supported
    ExternalLink externalLink =
        volumeOperationInfo.getExternalLinks() == null
            ? null
            : volumeOperationInfo.getExternalLinks().stream().findFirst().orElse(null);
    this.volumeOperationExecutor =
        new VolumeOperationExecutor(
            volumeOperationInfo.getVolumeOperationType(),
            externalLink,
            volumeOperationInfo.getLocalFile(),
            session
                .getClientInfoProperties()
                .getOrDefault(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ""),
            httpClient);
    Thread thread = new Thread(volumeOperationExecutor);
    thread.setName("VolumeOperationExecutor " + statementId);
    thread.start();
  }

  private void validateMetadata() throws SQLException {
    // For now we only support one row for Volume operation
    if (manifest.getTotalRowCount() > 1) {
      throw new DatabricksSQLException("Too many rows for Volume Operation");
    }
    ResultSchema schema = manifest.getSchema();
    if (schema.getColumnCount() > 4) {
      throw new DatabricksSQLException("Too many columns for Volume Operation");
    }
    if (schema.getColumnCount() < 3)  {
      throw new DatabricksSQLException("Too few columns for Volume Operation");
    }

  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (currentRowIndex < 0) {
      throw new DatabricksSQLException("Invalid row access");
    }
    if (columnIndex == 1) {
      return volumeOperationExecutor.getStatus().name();
    } else {
      throw new DatabricksSQLException("Invalid column access");
    }
  }

  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (hasNext()) {
      poll();
      currentRowIndex++;
      return true;
    } else {
      return false;
    }
  }

  private void poll() throws DatabricksSQLException {
    // TODO: handle timeouts
    while (volumeOperationExecutor.getStatus() == VolumeOperationStatus.PENDING
        || volumeOperationExecutor.getStatus() == VolumeOperationStatus.RUNNING) {
      try {
        Thread.sleep(DEFAULT_SLEEP_DELAY);
      } catch (InterruptedException e) {
        throw new DatabricksSQLException(
            "Thread interrupted while waiting for volume operation to complete", e);
      }
    }
    if (volumeOperationExecutor.getStatus() == VolumeOperationStatus.FAILED) {
      throw new DatabricksSQLException(
          "Volume operation failed: " + volumeOperationExecutor.getErrorMessage());
    }
    if (volumeOperationExecutor.getStatus() == VolumeOperationStatus.ABORTED) {
      throw new DatabricksSQLException(
          "Volume operation aborted: " + volumeOperationExecutor.getErrorMessage());
    }
  }

  @Override
  public boolean hasNext() {
    return currentRowIndex < 0;
  }

  @Override
  public void close() {
    // TODO: handle close, shall we abort the operation?
  }
}
