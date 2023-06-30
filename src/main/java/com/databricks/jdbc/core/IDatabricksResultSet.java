package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.StatementStatus;

public interface IDatabricksResultSet {
  String statementId();

  StatementStatus getStatementStatus();
}
