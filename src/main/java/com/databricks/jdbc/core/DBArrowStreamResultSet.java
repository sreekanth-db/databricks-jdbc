package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.sql.SQLException;

class DBArrowStreamResultSet extends DBChunkResultSet {
  DBArrowStreamResultSet(ResultManifest resultManifest, ResultData resultData) {
    super(resultManifest, resultData);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
