package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksDriver;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DatabricksDataSource implements DataSource {
  private String host;
  private int port;
  private String url;
  private Properties properties = new Properties();

  @Override
  public Connection getConnection() {
    return getConnection(this.getUsername(), this.getPassword());
  }

  @Override
  public Connection getConnection(String username, String password) {
    if (username != null) {
      setUsername(username);
    }
    if (password != null) {
      setPassword(password);
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
  public void setLoginTimeout(int seconds) {
    this.properties.put(DatabricksJdbcConstants.LOGIN_TIMEOUT, seconds);
  }

  @Override
  public int getLoginTimeout() {
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
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(DatabricksJdbcConstants.JDBC_SCHEMA);
    urlBuilder.append(host);
    if (port != 0) {
      urlBuilder.append(":").append(port);
    }
    this.url = urlBuilder.toString();
    return this.url;
  }

  public String getUsername() {
    return properties.getProperty(DatabricksJdbcConstants.USER);
  }

  public void setUsername(String username) {
    properties.put(DatabricksJdbcConstants.USER, username);
  }

  public String getPassword() {
    return properties.getProperty(DatabricksJdbcConstants.PASSWORD);
  }

  public void setPassword(String password) {
    properties.put(DatabricksJdbcConstants.PASSWORD, password);
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

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }
}
