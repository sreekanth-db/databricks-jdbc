package com.databricks.jdbc.integration.errorhandling;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ConcurrencyIntegrationTests {

    static String tableName = "test_table";

    @AfterEach
    void cleanUp() throws SQLException {
        String SQL =
                "DROP TABLE IF EXISTS "
                        + getDatabricksCatalog()
                        + "."
                        + getDatabricksSchema()
                        + "."
                        + tableName;
        executeSQL(SQL);
    }

    @Test
    void testConcurrencyOnMultipleThreads() throws SQLException {
        setUpDatabaseSchema(tableName);
        insertTestData();
        Connection connection = getValidJDBCConnection();
        Statement statement = connection.createStatement();
        Thread thread1 =
                new Thread(
                        () -> {
                            try {
                                statement.executeUpdate(
                                        "UPDATE "
                                                + getDatabricksCatalog()
                                                + "."
                                                + getDatabricksSchema()
                                                + "."
                                                + tableName
                                                + " SET col1 = 'value3' WHERE id = 1");
                            } catch (SQLException e) {
                                e.printStackTrace(); // Handle exception appropriately
                            }
                        });

        Thread thread2 =
                new Thread(
                        () -> {
                            try {
                                statement.executeUpdate(
                                        "UPDATE "
                                                + getDatabricksCatalog()
                                                + "."
                                                + getDatabricksSchema()
                                                + "."
                                                + tableName
                                                + " SET col2 = 'value4' WHERE id = 1");
                            } catch (SQLException e) {
                                e.printStackTrace(); // Handle exception appropriately
                            }
                        });

        thread1.start();
        thread2.start();

        DatabricksSQLException e = assertThrows(DatabricksSQLException.class, () -> {
            thread1.join();
            thread2.join();
        });

        assertTrue(e.getMessage().contains("Files were added to the root of the table by a concurrent update."));

//    ResultSet rs =
//        executeQuery(
//            "SELECT * FROM "
//                + getDatabricksCatalog()
//                + "."
//                + getDatabricksSchema()
//                + "."
//                + tableName
//                + " WHERE id = 1");
//    while (rs.next()) {
//      assertTrue(rs.getString("col1").equals("value3"));
//      assertTrue(rs.getString("col2").equals("value4"));
//    }
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

    private Connection getConnection(String url, String username, String password)
            throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
}
