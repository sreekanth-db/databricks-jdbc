package com.databricks.sql.client.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.*;
import com.databricks.sql.client.jdbc.DatabricksConnectionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;

@ExtendWith(MockitoExtension.class)
public class DatabricksSessionTest {

  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String JDBC_URL_INVALID = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehou/erg6767gg;";
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";

  @Mock
  StatementExecutionService statementExecutionService;

  @Test
  public void testOpenSession() {
    DatabricksSession session = new DatabricksSession(DatabricksConnectionContext.parse(JDBC_URL, new Properties()), statementExecutionService);

    CreateSessionRequest createSessionRequest = new CreateSessionRequest().setSession(new Session().setWarehouseId(WAREHOUSE_ID));
    when(statementExecutionService.createSession(createSessionRequest)).thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));

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
    assertThrows(IllegalArgumentException.class, () -> new DatabricksSession(DatabricksConnectionContext.parse(JDBC_URL_INVALID, new Properties()), statementExecutionService));
  }
}
