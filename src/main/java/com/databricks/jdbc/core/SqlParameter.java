package com.databricks.jdbc.core;

import org.immutables.value.Value;

@Value.Immutable
public interface SqlParameter {

  Object value();
  int type();
  int cardinal();
}
