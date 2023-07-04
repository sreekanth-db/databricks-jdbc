package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

class DBResultSetFactory {
  static IDBResultSet getResultSet(ResultData data, ResultManifest manifest) {
    switch (manifest.getFormat()) {
      case ARROW_STREAM:
        return new DBArrowStreamResultSet(manifest, data);
      case CSV:
        return new DBCsvResultSet(manifest, data);
      case JSON_ARRAY:
        // TODO: Add handling for Chunk based JSON result set
        return new DBInlineJsonResultSet(manifest, data);
      default:
        throw new IllegalStateException("Invalid response format " + manifest.getFormat());
    }
  }
}
