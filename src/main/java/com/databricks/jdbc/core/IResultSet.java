package com.databricks.jdbc.core;

interface IDBResultSet {
  Object getObject(int columnIndex);

  int getCurrentRow();

  boolean next();

  boolean previous();
}
