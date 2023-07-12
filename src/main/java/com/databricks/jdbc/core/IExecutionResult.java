package com.databricks.jdbc.core;

import java.sql.SQLException;

interface IExecutionResult {
  Object getObject(int columnIndex) throws SQLException;

  int getCurrentRow();

  boolean next();

  boolean previous();
}
