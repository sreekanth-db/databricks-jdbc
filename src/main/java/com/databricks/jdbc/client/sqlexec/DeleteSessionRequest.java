package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.support.Generated;
import com.databricks.sdk.support.ToStringer;
import java.util.Objects;

@Generated
public class DeleteSessionRequest {
  private String sessionId;

  public DeleteSessionRequest() {
  }

  public DeleteSessionRequest setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      DeleteSessionRequest that = (DeleteSessionRequest)o;
      return Objects.equals(this.sessionId, that.sessionId);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.sessionId});
  }

  public String toString() {
    return (new ToStringer(DeleteSessionRequest.class)).add("sessionId", this.sessionId).toString();
  }
}
