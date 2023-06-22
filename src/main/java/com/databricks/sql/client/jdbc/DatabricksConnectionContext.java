package com.databricks.sql.client.jdbc;

import static com.databricks.sql.client.jdbc.DatabricksJdbcConstants.JDBC_SCHEMA;
import static com.databricks.sql.client.jdbc.DatabricksJdbcConstants.PAIR_DELIMITER;
import static com.databricks.sql.client.jdbc.DatabricksJdbcConstants.PORT_DELIMITER;
import static com.databricks.sql.client.jdbc.DatabricksJdbcConstants.URL_DELIMITER;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DatabricksConnectionContext {

  private final String host;
  private final Integer port;
  private final Map<String, String> parameters;
  public static DatabricksConnectionContext parse(String url, Properties properties) {
    // TODO: handle exceptions properly
    if (!url.startsWith(JDBC_SCHEMA)) {
      throw new IllegalArgumentException("Invalid url " + url);
    }
    int hostEndingIndex = url.indexOf(URL_DELIMITER);
    if (hostEndingIndex < 0) {
      throw new IllegalArgumentException("Invalid url " + url);
    }
    // TODO: can we parse using URI.parse?
    String hostUrl = url.substring(JDBC_SCHEMA.length(), hostEndingIndex);
    String[] parsedHost = hostUrl.substring(0, hostUrl.indexOf("/")).split(PORT_DELIMITER);
    if (parsedHost.length > 2) {
      handleInvalidUrl(url);
    }
    String _host = parsedHost[0];
    Integer _port = parsedHost.length == 2 ? Integer.valueOf(parsedHost[1]) : null;
    Map<String, String> _parameters = new HashMap<>();
    for (String keyValuePair : url.substring(hostEndingIndex + 1).split(URL_DELIMITER)) {
      String[] pair = keyValuePair.split(PAIR_DELIMITER);
      if (pair.length != 2) {
        handleInvalidUrl(url);
      }
      _parameters.put(pair[0], pair[1]);
    }
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      _parameters.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return new DatabricksConnectionContext(_host, _port, _parameters);
  }

  private static void handleInvalidUrl(String url) {
    throw new IllegalArgumentException("Invalid url " + url);
  }

  private DatabricksConnectionContext(String host, Integer port, Map<String, String> parameters) {
    this.host = host;
    this.port = port;
    this.parameters = parameters;
  }

  public String getHostUrl() {
    StringBuilder hostUrlBuilder = new StringBuilder().append(DatabricksJdbcConstants.HTTPS_SCHEMA)
        .append(this.host);
    if (port != null) {
      hostUrlBuilder.append(PORT_DELIMITER)
          .append(port);
    }
    return hostUrlBuilder.toString();
  }

  public String getParameter(String key) {
    return this.parameters.getOrDefault(key, "");
  }
}
