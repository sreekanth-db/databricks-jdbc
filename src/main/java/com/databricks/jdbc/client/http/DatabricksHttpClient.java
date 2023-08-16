package com.databricks.jdbc.client.http;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;

public class DatabricksHttpClient implements IDatabricksHttpClient {

  private static final int DEFAULT_MAX_HTTP_CONNECTIONS = 300;
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 300;
  private static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 60 * 1000; // ms
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300 * 1000; // ms

  private static DatabricksHttpClient instance = null;

  private final PoolingHttpClientConnectionManager connectionManager;

  private final CloseableHttpClient hc;

  private DatabricksHttpClient() {
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE);
    hc = makeClosableHttpClient();
  }

  private RequestConfig makeRequestConfig() {
    return RequestConfig.custom()
        .setConnectionRequestTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setConnectTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
        .build();
  }

  private CloseableHttpClient makeClosableHttpClient() {
    return HttpClientBuilder.create()
        .setConnectionManager(connectionManager)
        // TODO: set appropriate user agent
        .setUserAgent("jdbc/databricks")
        .setDefaultRequestConfig(makeRequestConfig())
        .build();
  }

  public static synchronized DatabricksHttpClient getInstance() {
    if (instance == null) {
      instance = new DatabricksHttpClient();
    }
    return instance;
  }


  @Override
  public HttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    // TODO: add retries and error handling
    try {
      return hc.execute(request);
    } catch (IOException e) {
      // TODO: handle this and log
      throw new DatabricksHttpException(e.getMessage(), e);
    }
  }
}
