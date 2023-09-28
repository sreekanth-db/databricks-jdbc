package com.databricks.jdbc.core;

import java.sql.Types;

public class DatabricksTypes {

  public static final String BIGINT = "BIGINT";
  public static final String BINARY = "BINARY";
  public static final String BOOLEAN = "BOOLEAN";
  public static final String DATE = "DATE";
  public static final String DECIMAL = "DECIMAL";
  public static final String DOUBLE = "DOUBLE";
  public static final String FLOAT = "FLOAT";
  public static final String INT = "INT";
  public static final String INTERVAL = "INTERVAL";
  public static final String VOID = "VOID";
  public static final String SMALLINT = "SMALLINT";
  public static final String STRING = "STRING";
  public static final String TINYINT = "TINYINT";
  public static final String TIMESTAMP = "TIMESTAMP";
  public static final String TIMESTAMP_NTZ = "TIMESTAMP_NTZ";
  public static final String MAP = "MAP";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";

  public static String getDatabricksType(int type) {
    switch (type) {
      case Types.ARRAY:
        return ARRAY;
        case Types.BIGINT:
          return BIGINT;
          case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
            return BINARY;
      case Types.DATE:
        return DATE;
      case Types.DECIMAL:
        return DECIMAL;
      case Types.BOOLEAN:
        return BOOLEAN;
      case Types.DOUBLE:
        return DOUBLE;
      case Types.FLOAT:
        return FLOAT;
      case Types.INTEGER:
        return INT;
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
        return STRING;
      case Types.TIMESTAMP:
        return TIMESTAMP;
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return TIMESTAMP_NTZ;
      case Types.STRUCT:
        return STRUCT;
      case Types.TINYINT:
        return TINYINT;
      case Types.SMALLINT:
        return SMALLINT;
      default:
        return null;
    }
  }
}
