package com.databricks.jdbc.core;

import org.immutables.value.Value;

import javax.annotation.Nullable;

@Value.Immutable
public interface SqlParameter {

  @Nullable Object value();

  String type();

  int cardinal();
}
