package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
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
  private static final HashMap<String, Double> gaugeMetrics = new HashMap<>();
  private static final HashMap<String, Double> counterMetrics = new HashMap<>();

  private static long LastModified = System.currentTimeMillis();

  private static final int INTERVAL = 10 * 1000;

  private static long count_http_post = 0;

  private static DatabricksHttpClient telemetryClient = null;

  public static void setTelemetryClient(IDatabricksConnectionContext context) {
    telemetryClient = DatabricksHttpClient.getInstance(context);
  }

  private DatabricksMetrics() throws IOException {
    // Private constructor to prevent instantiation
  }

  public static String sendRequest(HashMap<String, Double> map) throws Exception {
    // Check if the telemetry client is set
    if (telemetryClient == null) {
      throw new DatabricksHttpException(
          "Telemetry client is not set. Initialize the Driver first.");
    }

    // Convert the map to JSON string
    ObjectMapper objectMapper = new ObjectMapper();
    String jsonInputString = objectMapper.writeValueAsString(map);

    // Create the request and adding parameters & headers
    URIBuilder uriBuilder = new URIBuilder(URL);
    uriBuilder.addParameter("metrics_map_string", jsonInputString);
    HttpUriRequest request = new HttpGet(uriBuilder.build());
    request.addHeader("Authorization", "Bearer " + ACCESS_TOKEN);

    // Execute the request and get the response
    CloseableHttpResponse response = telemetryClient.execute(request);

    // Error handling
    if (response == null) {
      throw new DatabricksHttpException("Response is null");
    } else if (response.getStatusLine().getStatusCode() != 200) {
      throw new DatabricksHttpException(
          "Response code is not 200. Response code: "
              + response.getStatusLine().getStatusCode()
              + " Response: "
              + response.getEntity().toString());
    }

    // Get the response string
    String responseString = EntityUtils.toString(response.getEntity());
    response.close();
    return responseString;
  }

  public static void postMetrics() {
    CompletableFuture.supplyAsync(
            () -> {
              long current_time = System.currentTimeMillis();

              if (current_time - LastModified >= INTERVAL) {
                LastModified = current_time;
                try {
                  sendRequest(gaugeMetrics);
                } catch (Exception e) {
                  System.out.println(e.getMessage());
                }
              }
              return null;
            })
        .thenAccept(
            response -> {
              if (response != null) {
                gaugeMetrics.clear();
                count_http_post += (System.currentTimeMillis() - LastModified);
              }
            });
  }

  public static void SetGaugeMetric(String name, double value) {
    // TODO: Handling metrics export when multiple users are accessing from the same workspace_id.
    if (!gaugeMetrics.containsKey(name)) {
      gaugeMetrics.put(name, 0.0);
    }
    gaugeMetrics.put(name, value);
    postMetrics();
  }

  public static void IncCounterMetric(String name, double value) {
    if (!counterMetrics.containsKey(name)) {
      counterMetrics.put(name, 0.0);
    }
    counterMetrics.put(name, counterMetrics.get(name) + value);
    postMetrics();
  }

  public static void record(String name, double value) {
    SetGaugeMetric(name, value);
  }

  public static double GetGaugeMetric(String name) {
    return gaugeMetrics.get(name);
  }

  public static double GetCounterMetric(String name) {
    return counterMetrics.get(name);
  }

  public static long getHttpLatency() {
    return count_http_post;
  }

  public static HashMap<String, Double> getGaugeMetrics() {
    return gaugeMetrics;
  }

  public static HashMap<String, Double> getCounterMetrics() {
    return counterMetrics;
  }
}
