package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksDriver;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DatabricksDataSource implements DataSource {

    private String username;
    private String password;
    private String host;
    private int port;
    private String url;
    private Properties properties = new Properties();

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(username, password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (username != null) {
            setUsername(username);
            properties.put(DatabricksJdbcConstants.USER, username);
        }
        if (password != null) {
            setPassword(password);
            properties.put(DatabricksJdbcConstants.PASSWORD, password);
        }
        return DatabricksDriver.INSTANCE.connect(getUrl(), properties);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException("public PrintWriter getLogWriter()");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new SQLFeatureNotSupportedException("public void setLogWriter(PrintWriter out)");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.properties.put(DatabricksJdbcConstants.LOGIN_TIMEOUT, seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return (int) this.properties.get(DatabricksJdbcConstants.LOGIN_TIMEOUT);
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("public Logger getParentLogger()");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public String getUrl() {
        if (url != null) {
            return url;
        }
        StringBuilder url = new StringBuilder();
        url.append(DatabricksJdbcConstants.JDBC_SCHEMA);
        url.append(host);
        if (port != 0) {
            url.append(":").append(port);
        }
        return url.toString();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
