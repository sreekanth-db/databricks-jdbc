package com.databricks.jdbc.integration.preparedstatement;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PreparedStatementIntegrationTests {

    private Connection connection;
    private static String tableName = "test_table";

    @BeforeEach
    void setUp() throws SQLException {
        connection = getValidJDBCConnection();
        setUpDatabaseSchema(tableName);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private static void insertTestData() throws SQLException {
        String insertSQL =
                "INSERT INTO "
                        + getDatabricksCatalog()
                        + "."
                        + getDatabricksSchema()
                        + "."
                        + tableName
                        + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
        executeSQL(insertSQL);
    }

    @Test
    void testPreparedStatementExecution() throws SQLException {
        insertTestData();
        String sql = "SELECT * FROM " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName + " WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 1); // Assuming 'id' is an integer
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next(), "Should return at least one result");
            }
        }
    }

    @Test
    void testParameterBindingInPreparedStatement() throws SQLException {
        String sql = "INSERT INTO " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName + "(id, col1, col2) VALUES (?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 2);
            statement.setString(2, "value1");
            statement.setString(3, "value2");
            int affectedRows = statement.executeUpdate();
            assertEquals(1, affectedRows, "One row should be inserted");
        }
    }

    @Test
    void testPreparedStatementComplexQueryExecution() throws SQLException {
        insertTestData();
        String sql = "UPDATE " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName + " SET col1 = ? WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, "Updated value");
            statement.setInt(2, 1);
            int affectedRows = statement.executeUpdate();
            assertEquals(1, affectedRows, "One row should be updated");
        }
    }

    @Test
    void testHandlingNullValuesWithPreparedStatement() throws SQLException {
        String sql = "INSERT INTO " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName + "(id, col1, col2) VALUES (?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 6);
            statement.setNull(2, java.sql.Types.VARCHAR);
            statement.setString(3, "value1");
            int affectedRows = statement.executeUpdate();
            assertEquals(1, affectedRows, "One row should be inserted with a null col1");
        }
    }
}
