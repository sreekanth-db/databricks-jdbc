package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.DatabricksRetryHandlerException;
import com.databricks.jdbc.client.http.RequestSanitizer;
import com.databricks.jdbc.commons.LogLevel;
import org.apache.http.client.methods.HttpUriRequest;

public class HttpExecuteExceptionUtil {
  public static void throwException(Exception e, HttpUriRequest request)
      throws DatabricksHttpException {
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof DatabricksRetryHandlerException) {
        throw new DatabricksHttpException(cause.getMessage(), cause);
      }
      cause = cause.getCause();
    }
    String errorMsg =
        String.format(
            "Caught error while executing http request: [%s]. Error Message: [%s]",
            RequestSanitizer.sanitizeRequest(request), e);
    LoggingUtil.log(LogLevel.ERROR, errorMsg);
    throw new DatabricksHttpException(errorMsg, e);
  }
}
