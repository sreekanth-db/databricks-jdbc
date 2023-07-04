package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

public class DBCsvResultSet extends DBChunkResultSet {
  DBCsvResultSet(ResultManifest resultManifest, ResultData resultData) {
    super(resultManifest, resultData);
  }

  @Override
  public Object getObject(int columnIndex) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
