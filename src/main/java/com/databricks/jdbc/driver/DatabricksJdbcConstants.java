package com.databricks.jdbc.driver;

import java.util.Set;
import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {

  static final Pattern JDBC_URL_PATTERN =
      Pattern.compile("jdbc:databricks://([^/;]*)(?::\\d+)?/*(.*)");
  static final Pattern HTTP_PATH_PATTERN = Pattern.compile(".*/warehouses/(.*)");
  static final Pattern HTTP_PATH_SQL_PATTERN = Pattern.compile("sql/(.*)");
  public static final String JDBC_SCHEMA = "jdbc:databricks://";
  static final String DEFAULT_LOG_LEVEL = "INFO";
  static final String LOG_LEVEL = "loglevel";
  static final String LOG_PATH = "logpath";
  static final String SYSTEM_LOG_LEVEL_CONFIG = "defaultLogLevel";
  static final String SYSTEM_LOG_FILE_CONFIG = "defaultLogFile";
  static final String URL_DELIMITER = ";";
  static final String PORT_DELIMITER = ":";
  static final String PAIR_DELIMITER = "=";
  static final String TOKEN = "token";
  public static final String USER = "user";
  public static final String PASSWORD = "password";

  static final String CLIENT_ID = "databricks_client_id";

  static final String CLIENT_SECRET = "databricks_client_secret";

  static final String AUTH_MECH = "authmech";

  static final String CONN_CATALOG = "conncatalog";

  static final String CONN_SCHEMA = "connschema";

  static final String AUTH_FLOW = "auth_flow";

  // Only used when AUTH_MECH = 11
  static final String AUTH_ACCESSTOKEN = "auth_accesstoken";

  // Only used when AUTH_MECH = 3
  static final String UID = "uid";
  static final String PWD = "pwd";

  static final String AWS_CLIENT_ID = "databricks-sql-jdbc";

  static final String AAD_CLIENT_ID = "96eecda7-19ea-49cc-abb5-240097d554f5";

  static final String HTTP_PATH = "httppath";
  static final String HTTPS_SCHEMA = "https://";
  public static final String LOGIN_TIMEOUT = "loginTimeout";

  public static final String U2M_AUTH_TYPE = "external-browser";
  public static final String M2M_AUTH_TYPE = "oauth-m2m";
  public static final String ACCESS_TOKEN_AUTH_TYPE = "pat";

  public static final String U2M_AUTH_REDIRECT_URL = "http://localhost:8020";

  public static final String SQL_SCOPE = "sql";
  public static final String OFFLINE_ACCESS_SCOPE = "offline_access";
  public static final String FULL_STOP = ".";
  public static final String EMPTY_STRING = "";
  public static final String IDENTIFIER_QUOTE_STRING = "`";
  public static final String CATALOG = "catalog";
  public static final String PROCEDURE = "procedure";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String USER_NAME = "User";
  static final int DEFAULT_PORT = 443;

  static final String LZ4_COMPRESSION_FLAG =
      "EnableQueryResultLZ4Compression"; // Adding this for backward compatibility only
  static final String COMPRESSION_FLAG = "QueryResultCompressionType";
  static final String USER_AGENT_ENTRY = "useragententry";
  public static final String DEFAULT_USER_AGENT = "DatabricksJDBCDriverOSS";
  static final String CLIENT_USER_AGENT_PREFIX = "Java";
  static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient/HC";
  static final String USER_AGENT_THRIFT_CLIENT = "THttpClient/HC";
  static final Set<String> ALLOWED_SESSION_CONFIGS =
      Set.of(
          "spark.databricks.sqlgateway.useCreateViewCommandWithResult",
          "spark.sql.thriftserver.metadata.column.singleschema",
          "spark.sql.thriftserver.metadata.table.singleschema",
          "spark.sql.crossJoin.enabled",
          // ES-52047: Tableau Online uses the Simba ODBC 2.6.4 driver
          // which incorrectly converts SSP properties to uppercase.
          "SPARK.SQL.CROSSJOIN.ENABLED",
          "spark.thriftserver.cloudStoreBasedRowSet.enabled",
          "spark.thriftserver.cloudfetch.enabled",
          // Deprecated, but still being set in the Tableau connector
          "SSP_databricks.catalog",
          "databricks.catalog",
          "spark.databricks.sql.readOnly",
          "spark.thriftserver.arrowBasedRowSet.timestampAsString");
}
