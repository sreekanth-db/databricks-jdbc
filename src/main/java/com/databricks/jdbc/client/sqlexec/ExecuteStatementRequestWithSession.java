package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ExecuteStatementRequestWithSession extends ExecuteStatementRequest {

  /** session-id */
  @JsonProperty("session_id")
  private String sessionId;

  public ExecuteStatementRequestWithSession setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      ExecuteStatementRequestWithSession that = (ExecuteStatementRequestWithSession) o;
      return Objects.equals(this.getByteLimit(), that.getByteLimit())
          && Objects.equals(this.getCatalog(), that.getCatalog())
          && Objects.equals(this.getDisposition(), that.getDisposition())
          && Objects.equals(this.getFormat(), that.getFormat())
          && Objects.equals(this.getOnWaitTimeout(), that.getOnWaitTimeout())
          && Objects.equals(this.getSchema(), that.getSchema())
          && Objects.equals(this.sessionId, that.sessionId)
          && Objects.equals(this.getStatement(), that.getStatement())
          && Objects.equals(this.getWaitTimeout(), that.getWaitTimeout())
          && Objects.equals(this.getWarehouseId(), that.getWarehouseId());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {

    return Objects.hash(
        super.hashCode(),
        sessionId);
  }

  @Override
  public String toString() {
    return new ToStringer(ExecuteStatementRequest.class)
        .add("byteLimit", super.getByteLimit())
        .add("catalog", super.getCatalog())
        .add("disposition", super.getDisposition())
        .add("format", super.getFormat())
        .add("onWaitTimeout", super.getOnWaitTimeout())
        .add("schema", super.getSchema())
        .add("sessionId", sessionId)
        .add("statement", super.getStatement())
        .add("waitTimeout", super.getWaitTimeout())
        .add("warehouseId", super.getWarehouseId())
        .toString();
  }
}
