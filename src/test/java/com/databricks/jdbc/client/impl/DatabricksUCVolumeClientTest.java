package com.databricks.jdbc.client.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DatabricksUCVolumeClientTest {

  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private ResultSet mockResultSet;

  private DatabricksUCVolumeClient client;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    client = new DatabricksUCVolumeClient(mockConnection);
  }

  @Test
  public void testPrefixExists() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false); // Simulate finding one result, then end
    when(mockResultSet.getString("name")).thenReturn("testPrefix");

    boolean exists =
        client.prefixExists("testCatalog", "testSchema", "testVolume", "testPrefix", true);

    assertTrue(exists);
  }

  @Test
  public void testObjectExists() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false); // Simulate finding one result, then end
    when(mockResultSet.getString("name")).thenReturn("testObject");

    boolean exists =
        client.objectExists("testCatalog", "testSchema", "testVolume", "testObject", true);

    assertTrue(exists);
  }

  @Test
  public void testVolumeDoesNotExist() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false); // Simulate no results found

    boolean exists = client.volumeExists("testCatalog", "testSchema", "nonExistingVolume", true);

    assertFalse(exists);
  }

  @Test
  public void testListObjectsWithMatchingPrefix() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next())
        .thenReturn(true, true, false); // Simulate finding two results, then end
    when(mockResultSet.getString("name")).thenReturn("testPrefixObject1", "testPrefixObject2");

    List<String> objects =
        client.listObjects("testCatalog", "testSchema", "testVolume", "testPrefix", true);

    assertEquals(2, objects.size());
    assertTrue(objects.contains("testPrefixObject1"));
    assertTrue(objects.contains("testPrefixObject2"));
  }

  @Test
  public void testSQLExceptionHandlingForObjectExists() throws Exception {
    when(mockStatement.executeQuery(anyString()))
        .thenThrow(new java.sql.SQLException("Database error"));

    assertThrows(
        SQLException.class,
        () -> client.objectExists("testCatalog", "testSchema", "testVolume", "testObject", true));
  }

  @Test
  public void testPrefixExistsSQLException() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenThrow(new SQLException("Database error"));

    assertThrows(
        SQLException.class,
        () -> client.prefixExists("testCatalog", "testSchema", "testVolume", "testPrefix", true));
  }

  @Test
  public void testVolumeExistsSQLException() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenThrow(new SQLException("Database error"));

    assertThrows(
        SQLException.class,
        () -> client.volumeExists("testCatalog", "testSchema", "nonExistingVolume", true));
  }

  @Test
  public void testListObjectsSQLException() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenThrow(new SQLException("Database error"));

    assertThrows(
        SQLException.class,
        () -> client.listObjects("testCatalog", "testSchema", "testVolume", "testPrefix", true));
  }
}
