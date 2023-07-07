package com.databricks.jdbc.core;

import java.sql.SQLException;

interface IDBResultSet {
  Object getObject(int columnIndex) throws SQLException;

  int getCurrentRow();

  boolean next();

  boolean previous();
}
