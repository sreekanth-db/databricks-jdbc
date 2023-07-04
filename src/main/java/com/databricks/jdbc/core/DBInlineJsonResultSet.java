package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class DBInlineJsonResultSet implements IDBResultSet {

  private int currentRow;
  private Collection<Collection<String>> data;
  private ResultManifest resultManifest;
  private ResultData resultData;

  DBInlineJsonResultSet(ResultManifest resultManifest, ResultData resultData) {
    this.resultManifest = resultManifest;
    this.resultData = resultData;
    this.data = resultData.getDataArray();
    this.currentRow = 0;
  }

  @Override
  public Object getObject(int columnIndex) {
    return null;
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
