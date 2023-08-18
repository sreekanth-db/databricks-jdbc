package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.util.Properties;

import com.databricks.jdbc.client.FakeDatabricksClient;
import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.CreateSessionRequest;
import com.databricks.sdk.service.sql.Session;
import com.databricks.sdk.service.sql.StatementExecutionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksConnectionTest {

  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";

  @Mock
  StatementExecutionService statementExecutionService;


//  public void testConnection() throws Exception {
//    CreateSessionRequest createSessionRequest = new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
//    when(statementExecutionService.createSession(createSessionRequest))
//        .thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));
//
//    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
//    DatabricksConnection connection = new DatabricksConnection(connectionContext,
//        new DatabricksSdkClient(connectionContext, statementExecutionService));
//
//    assertFalse(connection.isClosed());
//    assertEquals(connection.getSession().getSessionId(), SESSION_ID);
//  }
}