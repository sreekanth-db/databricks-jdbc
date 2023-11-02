package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.StatementExecutionService;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksConnectionTest {

  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";

  @Mock StatementExecutionService statementExecutionService;

  @Mock ApiClient apiClient;

  @Test
  public void testConnection() throws Exception {
    CreateSessionRequest createSessionRequest =
        new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json");
    when(apiClient.POST(
            "/api/2.0/sql/statements/sessions", createSessionRequest, Session.class, headers))
        .thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection =
        new DatabricksConnection(
            connectionContext,
            new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));

    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionId(), SESSION_ID);
  }
}
