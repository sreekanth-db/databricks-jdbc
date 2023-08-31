package com.databricks.jdbc.pooling;

import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.CreateSessionRequest;
import com.databricks.sdk.service.sql.Session;
import com.databricks.sdk.service.sql.StatementExecutionService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DatabricksConnectionPoolingTest {

    private static final String JDBC_URL = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    private static final String WAREHOUSE_ID = "791ba2a31c7fd70a";
    private static final String SESSION_ID = "session_id";
    private static final StatementExecutionService statementExecutionService = Mockito.mock(StatementExecutionService.class);

    @BeforeAll public static void setUp() {
        CreateSessionRequest createSessionRequest = new CreateSessionRequest().setWarehouseId(WAREHOUSE_ID);
        Mockito.when(statementExecutionService.createSession(createSessionRequest))
                .thenReturn(new Session().setWarehouseId(WAREHOUSE_ID).setSessionId(SESSION_ID));
    }

    @Test
    public void testPooledConnection() throws SQLException {
        DatabricksConnectionPoolDataSource poolDataSource = Mockito.mock(DatabricksConnectionPoolDataSource.class);

        IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(JDBC_URL, new Properties());
        DatabricksConnection databricksConnection = new DatabricksConnection(connectionContext,
                new DatabricksSdkClient(connectionContext, statementExecutionService));
        Mockito.when(poolDataSource.getPooledConnection()).thenReturn(new DatabricksPooledConnection(databricksConnection));

        PooledConnection pooledConnection = poolDataSource.getPooledConnection();
        TestListener listener = new TestListener();
        pooledConnection.addConnectionEventListener(listener);

        Connection connection = pooledConnection.getConnection();

        connection.close();
        List<ConnectionEvent> connectionClosedEvents = listener.getConnectionClosedEvents();
        Assertions.assertEquals(connectionClosedEvents.size(), 1);
        Connection actualConnection =
                ((DatabricksPooledConnection) pooledConnection).getActualConnection();
        Assertions.assertFalse(actualConnection.isClosed());

        pooledConnection.removeConnectionEventListener(listener);
        // TODO: Connection close currently throws an error on close - missing warehouse ID, uncomment below once fixed
//        pooledConnection.close();
//        Assertions.assertTrue(actualConnection.isClosed());
    }
}

class TestListener implements ConnectionEventListener {
    List<ConnectionEvent> connectionClosedEvents = new ArrayList<>();
    List<ConnectionEvent> connectionErrorEvents = new ArrayList<>();

    @Override
    public void connectionClosed(ConnectionEvent event) {
        connectionClosedEvents.add(event);
    }

    @Override
    public void connectionErrorOccurred(ConnectionEvent event) {
        connectionErrorEvents.add(event);
    }
    public List<ConnectionEvent> getConnectionClosedEvents() {
        return connectionClosedEvents;
    }

    public List<ConnectionEvent> getConnectionErrorEvents() {
        return connectionErrorEvents;
    }
}
