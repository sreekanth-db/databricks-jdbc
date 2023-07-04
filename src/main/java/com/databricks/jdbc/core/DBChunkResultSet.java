package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.util.Collection;

// TODO: Add chunk specific methods
abstract class DBChunkResultSet implements IDBResultSet {

  private int currentRow = -1;
  private Collection<Collection<String>> data;
  private ResultManifest resultManifest;
  private ResultData resultData;

  DBChunkResultSet(ResultManifest resultManifest, ResultData resultData) {
    this.resultManifest = resultManifest;
    this.resultData = resultData;
    this.currentRow = 0;
  }

  @Override
  public synchronized int getCurrentRow() {
    return currentRow;
  }

  @Override
  public synchronized boolean next() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized boolean previous() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
