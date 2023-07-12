package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

class DBExecutionResultFactory {
  static IExecutionResult getResultSet(ResultData data, ResultManifest manifest) {
    // We will use Arrow Stream only in prod. JSON is for testing and prototype purpose
    switch (manifest.getFormat()) {
      case ARROW_STREAM:
        return new DBArrowStreamResult(manifest, data);
      case JSON_ARRAY:
        // This is only for testing and prototype
        return new DBInlineJsonResult(manifest, data);
      default:
        throw new IllegalStateException("Invalid response format " + manifest.getFormat());
    }
  }
}
