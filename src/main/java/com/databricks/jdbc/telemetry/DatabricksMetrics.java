package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

public class DatabricksMetrics {
  private static final String URL =
      "https://test-shard-bhuvan-v2.dev.azuredatabricks.net/api/2.0/example-v2/exportMetrics";
  // TODO: Replace ACCESS_TOKEN with your own token - TO BE DECIDED ONCE THE SERVICE IS CREATED
  private static final String ACCESS_TOKEN = "x";
  private static final Map<String, Double> gaugeMetrics = new HashMap<>();
  private static final Map<String, Double> counterMetrics = new HashMap<>();
  private static long lastSuccessfulHttpReq = System.currentTimeMillis();
  private static final long intervalDurationForSendingReq = TimeUnit.SECONDS.toMillis(10 * 60);
  private static long httpLatency = 0;
  private static DatabricksHttpClient telemetryClient = null;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String METRICS_MAP_STRING = "metrics_map_string";
  private static final String METRICS_TYPE = "metrics_map_int";

  public enum MetricsType {
    GAUGE,
    COUNTER
  }

  public static void instantiateTelemetryClient(IDatabricksConnectionContext context) {
    telemetryClient = DatabricksHttpClient.getInstance(context);
  }

  private DatabricksMetrics() throws IOException {
    // Private constructor to prevent instantiation
  }

  public static String sendRequest(Map<String, Double> map, MetricsType metricsType)
      throws Exception {
    // Check if the telemetry client is set
    if (telemetryClient == null) {
      throw new DatabricksHttpException(
          "Telemetry client is not set. Initialize the Driver first.");
    }

    // Convert the map to JSON string
    String jsonInputString = objectMapper.writeValueAsString(map);

    // Create the request and adding parameters & headers
    URIBuilder uriBuilder = new URIBuilder(URL);
    uriBuilder.addParameter(METRICS_MAP_STRING, jsonInputString);
    uriBuilder.addParameter(METRICS_TYPE, metricsType.name().equals("GAUGE") ? "1" : "0");
    HttpUriRequest request = new HttpGet(uriBuilder.build());
    request.addHeader("Authorization", "Bearer " + ACCESS_TOKEN);

    // Execute the request and get the response
    CloseableHttpResponse response = telemetryClient.execute(request);

    // Error handling
    if (response == null) {
      throw new DatabricksHttpException("Response is null");
    } else if (response.getStatusLine().getStatusCode() != 200) {
      throw new DatabricksHttpException(
          "Response code: "
              + response.getStatusLine().getStatusCode()
              + " Response: "
              + response.getEntity().toString());
    }

    // Get the response string
    String responseString = EntityUtils.toString(response.getEntity());
    response.close();
    return responseString;
  }

  public static void postMetrics(MetricsType metricsType) {
    CompletableFuture.supplyAsync(
            () -> {
              long currentTimeMillis = System.currentTimeMillis();

              if (currentTimeMillis - lastSuccessfulHttpReq >= intervalDurationForSendingReq) {
                try {
                  if (metricsType == MetricsType.GAUGE) {
                    sendRequest(gaugeMetrics, MetricsType.GAUGE);
                  } else {
                    sendRequest(counterMetrics, MetricsType.COUNTER);
                  }
                  lastSuccessfulHttpReq = System.currentTimeMillis();
                  httpLatency += (lastSuccessfulHttpReq - currentTimeMillis);
                } catch (Exception e) {
                  // Commenting out the exception for now - failing silently
                  System.out.println(e.getMessage());
                }
              }
              return null;
            })
        .thenAccept(
            response -> {
              if (response != null) {
                if (metricsType == MetricsType.GAUGE) {
                  gaugeMetrics.clear();
                } else {
                  counterMetrics.clear();
                }
              }
            });
  }

  public static void setGaugeMetrics(String name, double value) {
    // TODO: Handling metrics export when multiple users are accessing from the same workspace_id.
    if (!gaugeMetrics.containsKey(name)) {
      gaugeMetrics.put(name, 0.0);
    }
    gaugeMetrics.put(name, value);
    postMetrics(MetricsType.GAUGE);
  }

  public static void incCounterMetrics(String name, double value) {
    if (!counterMetrics.containsKey(name)) {
      counterMetrics.put(name, 0.0);
    }
    counterMetrics.put(name, value);
    postMetrics(MetricsType.COUNTER);
  }

  public static void record(String name, double value) {
    setGaugeMetrics(name, value);
  }

  public static void increment(String name, double value) {
    incCounterMetrics(name, value);
  }

  public static long getHttpLatency() {
    return httpLatency;
  }
}
