package com.databricks.jdbc.core;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class DBInlineJsonResult implements IExecutionResult {

  private int currentRow;
  private List<List<String>> data;
  private ResultManifest resultManifest;
  private ResultData resultData;

  DBInlineJsonResult(ResultManifest resultManifest, ResultData resultData) {
    this.resultManifest = resultManifest;
    this.resultData = resultData;
    this.data = getDataList(resultData.getDataArray());
    this.currentRow = 0;
  }

  private static List<List<String>> getDataList(Collection<Collection<String>> dataArray) {
    return dataArray.stream().map(c -> c.stream().collect(toImmutableList())).collect(toImmutableList());
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (columnIndex < data.get(currentRow).size()) {
      return data.get(currentRow).get(columnIndex);
    }
    throw new DatabricksSQLException("Column index out of bounds " + (columnIndex -1));
  }

  @Override
  public synchronized int getCurrentRow() {
    return currentRow + 1;
  }

  @Override
  public synchronized boolean next() {
    // TODO: handle pagination
    if (currentRow < data.size() -1) {
      currentRow++;
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean previous() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
