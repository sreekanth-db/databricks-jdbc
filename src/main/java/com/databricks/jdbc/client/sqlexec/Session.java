package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Session */
public class Session {
  /** */
  @JsonProperty("session_id")
  private String sessionId;

  /** */
  @JsonProperty("warehouse_id")
  private String warehouseId;

  public Session setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }

  public Session setWarehouseId(String warehouseId) {
    this.warehouseId = warehouseId;
    return this;
  }

  public String getWarehouseId() {
    return warehouseId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Session that = (Session) o;
    return Objects.equals(sessionId, that.sessionId)
        && Objects.equals(warehouseId, that.warehouseId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionId, warehouseId);
  }

  @Override
  public String toString() {
    return new ToStringer(Session.class)
        .add("sessionId", sessionId)
        .add("warehouseId", warehouseId)
        .toString();
  }
}
