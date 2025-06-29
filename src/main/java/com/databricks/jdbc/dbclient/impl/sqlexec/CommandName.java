package com.databricks.jdbc.dbclient.impl.sqlexec;

public enum CommandName {
  OPEN_SESSION,
  CLOSE_SESSION,
  LIST_TYPE_INFO,
  LIST_CATALOGS,
  LIST_TABLES,
  LIST_PRIMARY_KEYS,
  LIST_SCHEMAS,
  LIST_TABLE_TYPES,
  LIST_COLUMNS,
  LIST_FUNCTIONS,
  LIST_FOREIGN_KEYS
}
