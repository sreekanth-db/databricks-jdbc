package com.databricks.jdbc.client;

public enum StatementType {
  // Default, not to be used
  NONE,
  // Query statement
  QUERY,
  // Generic SQL statement, can be both DDL and DML
  SQL,
  // DML statement
  UPDATE,
  // metadata statement, this is similar to query, but provided internally and not user provided
  METADATA;
}
