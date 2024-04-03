package com.databricks.jdbc;

import com.databricks.jdbc.client.impl.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.client.impl.thrift.generated.TSessionHandle;
import com.databricks.jdbc.core.ImmutableSessionInfo;
import com.databricks.jdbc.core.types.AllPurposeCluster;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import java.util.Collections;
import java.util.Map;

/*All the common test constants should be placed here */
public class TestConstants {
  public static final String WAREHOUSE_ID = "warehouse_id";
  public static final String SESSION_ID = "12345678";
  private static final String CATALOG = "field_demos";
  private static final String SCHEMA = "ossjdbc";
  public static final Warehouse WAREHOUSE_COMPUTE = new Warehouse(WAREHOUSE_ID);
  public static final ComputeResource CLUSTER_COMPUTE =
      new AllPurposeCluster("6051921418418893", "1115-130834-ms4m0yv");
  public static final String TEST_SCHEMA = "testSchema";
  public static final String TEST_TABLE = "testTable";
  public static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
  public static final String TEST_COLUMN = "testColumn";
  public static final String TEST_CATALOG = "catalog1";
  public static final String TEST_FUNCTION_PATTERN = "functionPattern";
  public static final String TEST_STRING = "test";
  public static final TSessionHandle SESSION_HANDLE =
      new TSessionHandle().setSessionId(new THandleIdentifier().setGuid(SESSION_ID.getBytes()));
  public static final ImmutableSessionInfo SESSION_INFO =
      ImmutableSessionInfo.builder()
          .sessionHandle(SESSION_HANDLE)
          .sessionId(SESSION_ID)
          .computeResource(CLUSTER_COMPUTE)
          .build();
  public static final String WAREHOUSE_JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse_id;";
}
