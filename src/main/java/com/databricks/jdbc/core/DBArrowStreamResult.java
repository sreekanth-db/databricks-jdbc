package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.sql.SQLException;

class DBArrowStreamResult implements IExecutionResult {
  DBArrowStreamResult(ResultManifest resultManifest, ResultData resultData) {
    // TODO: Add impl
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getCurrentRow() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean previous() {
    return false;
  }
}
