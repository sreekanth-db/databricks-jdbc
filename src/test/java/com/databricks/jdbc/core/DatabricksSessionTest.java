package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.StatementExecutionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DatabricksSessionTest {

  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String JDBC_URL_INVALID = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehou/erg6767gg;";
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";

  @Mock
  StatementExecutionService statementExecutionService;

  @Mock
  ApiClient apiClient;

  @Test
  public void testOpenSession() {
    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext,
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));

    CreateSessionRequest createSessionRequest = new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json");
    when(apiClient.POST("adb-565757575.18.azuredatabricks.net/api/2.0/sql/statements/sessions", createSessionRequest,
        Session.class, headers)).thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));

    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());

    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testOpenSession_invalidWarehouseUrl() {
    assertThrows(IllegalArgumentException.class,
        () -> new DatabricksSession(DatabricksConnectionContext.parse(JDBC_URL_INVALID, new Properties()),
            null));
  }
}
