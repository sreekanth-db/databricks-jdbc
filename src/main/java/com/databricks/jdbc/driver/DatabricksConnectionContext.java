package com.databricks.jdbc.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

public class DatabricksConnectionContext {

  private final String host;
  private final int port;
  @VisibleForTesting
  final ImmutableMap<String, String> parameters;
  public static DatabricksConnectionContext parse(String url, Properties properties) {
    if (!isValid(url)) {
      // TODO: handle exceptions properly
      throw new IllegalArgumentException("Invalid url " + url);
    }
    Matcher urlMatcher = DatabricksJdbcConstants.JDBC_URL_PATTERN.matcher(url);

    if (urlMatcher.find()) {
      String hostUrlVal = urlMatcher.group(1);
      String urlMinusHost = urlMatcher.group(2);

      String[] hostAndPort = hostUrlVal.split(DatabricksJdbcConstants.PORT_DELIMITER);
      String hostValue = hostAndPort[0];
      int portValue = hostAndPort.length == 2 ? Integer.valueOf(hostAndPort[1]) : DatabricksJdbcConstants.DEFAULT_PORT;

      ImmutableMap.Builder<String, String> parametersBuilder = ImmutableMap.builder();
      String[] urlParts = urlMinusHost.split(DatabricksJdbcConstants.URL_DELIMITER);
      for (int urlPartIndex = 1; urlPartIndex < urlParts.length; urlPartIndex++) {
        String[] pair = urlParts[urlPartIndex].split(DatabricksJdbcConstants.PAIR_DELIMITER);
        if (pair.length != 2) {
          handleInvalidUrl(url);
        }
        parametersBuilder.put(pair[0], pair[1]);
      }
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        parametersBuilder.put(entry.getKey().toString(), entry.getValue().toString());
      }
      return new DatabricksConnectionContext(hostValue, portValue, parametersBuilder.build());
    } else {
      // Should never reach here, since we have already checked for url validity
      throw new IllegalArgumentException("Invalid url " + "incorrect");
    }
  }

  private static void handleInvalidUrl(String url) {

  }

  private DatabricksConnectionContext(String host, int port, ImmutableMap<String, String> parameters) {
    this.host = host;
    this.port = port;
    this.parameters = parameters;
  }

  public static boolean isValid(String url) {
    return DatabricksJdbcConstants.JDBC_URL_PATTERN.matcher(url).matches();
  }

  public String getHostUrl() {
    StringBuilder hostUrlBuilder = new StringBuilder().append(DatabricksJdbcConstants.HTTPS_SCHEMA)
        .append(this.host);
    if (port != 0) {
      hostUrlBuilder.append(DatabricksJdbcConstants.PORT_DELIMITER)
          .append(port);
    }
    return hostUrlBuilder.toString();
  }

  public String getHttpPath() {
    return getParameter(DatabricksJdbcConstants.HTTP_PATH);
  }

  public String getToken() {
    return getParameter(DatabricksJdbcConstants.TOKEN);
  }

  private String getParameter(String key) {
    return this.parameters.getOrDefault(key, null);
  }
}
