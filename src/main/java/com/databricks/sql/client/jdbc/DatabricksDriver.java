package com.databricks.sql.client.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * Databricks JDBC driver.
 * TODO: Add implementation to accept Urls in format: jdbc:databricks://host:port.
 */
public class DatabricksDriver implements Driver {
    private static final DatabricksDriver INSTANCE;

    private static int majorVersion = 0;
    private static int minorVersion = 0;

    static {
        try {
            DriverManager.registerDriver(INSTANCE = new DatabricksDriver());
            System.out.printf("Driver has been registered. instance = %s\n", INSTANCE);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to register " + DatabricksDriver.class, e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return false;
    }

    @Override
    public Connection connect(String url, Properties info) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMajorVersion() {
        return majorVersion;
    }

    @Override
    public int getMinorVersion() {
        return minorVersion;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return null;
    }

    public static final void main(String[] args) {
        System.out.printf("The driver %s has been initialized.\n", DatabricksDriver.class);
    }
}
