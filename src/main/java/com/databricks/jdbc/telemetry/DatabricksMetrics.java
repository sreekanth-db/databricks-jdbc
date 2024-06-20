package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
  public static final Map<String, Double> gaugeMetrics = new HashMap<>();
  public static final Map<String, Double> counterMetrics = new HashMap<>();
  private static final long intervalDurationForSendingReq =
      TimeUnit.SECONDS.toMillis(10 * 60); // 10 minutes
  private static DatabricksHttpClient telemetryClient = null;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String METRICS_MAP_STRING = "metrics_map_string";
  private static final String METRICS_TYPE = "metrics_map_int";
  private static Boolean firstExport = false;
  private static String warehouseId = null;

  public enum MetricsType {
    GAUGE,
    COUNTER
  }

  private static void scheduleExportMetrics() {
    Timer metricsTimer = new Timer();
    TimerTask task =
        new TimerTask() {
          @Override
          public void run() {
            try {
              sendRequest(gaugeMetrics, MetricsType.GAUGE);
              sendRequest(counterMetrics, MetricsType.COUNTER);
            } catch (Exception e) {
              // Commenting out the exception for now - failing silently
              // System.out.println(e.getMessage());
            }
          }
        };

    // Schedule the task to run after the specified interval infinitely
    metricsTimer.schedule(task, 0, intervalDurationForSendingReq);
  }

  public static void instantiateTelemetryClient(IDatabricksConnectionContext context) {
    telemetryClient = DatabricksHttpClient.getInstance(context);

    // Schedule the task using a parallel thread to send metrics to server every
    // "intervalDurationForSendingReq" seconds
    scheduleExportMetrics();
  }

  public static void setWarehouseId(String workspaceId) {
    DatabricksMetrics.warehouseId = workspaceId;
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

    // Return if the map is empty - prevents sending empty metrics & unnecessary API calls
    if (map.isEmpty()) {
      return "Metrics map is empty";
    }

    // Convert the map to JSON string
    String jsonInputString = objectMapper.writeValueAsString(map);

    // Create the request and adding parameters & headers
    URIBuilder uriBuilder = new URIBuilder(URL);
    uriBuilder.addParameter(METRICS_MAP_STRING, jsonInputString);
    uriBuilder.addParameter(METRICS_TYPE, metricsType.name().equals("GAUGE") ? "1" : "0");
    HttpUriRequest request = new HttpGet(uriBuilder.build());
    request.addHeader("Authorization", "Bearer " + ACCESS_TOKEN);
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
    } else {
      // Clearing map after successful response
      map.clear();
    }

    // Get the response string
    String responseString = EntityUtils.toString(response.getEntity());
    response.close();
    return responseString;
  }

  public static void setGaugeMetrics(String name, double value) {
    // TODO: Handling metrics export when multiple users are accessing from the same workspace_id.
    if (!gaugeMetrics.containsKey(name)) {
      gaugeMetrics.put(name, 0.0);
    }
    gaugeMetrics.put(name, value);
  }

  public static void incCounterMetrics(String name, double value) {
    if (!counterMetrics.containsKey(name)) {
      counterMetrics.put(name, 0.0);
    }
    counterMetrics.put(name, value);
  }

  private static void FirstExport(Map<String, Double> map, MetricsType metricsType) {
    if (firstExport) return;
    firstExport = true;
    CompletableFuture.runAsync(
        () -> {
          try {
            sendRequest(map, metricsType);
          } catch (Exception e) {
            // Commenting out the exception for now - failing silently
            // System.out.println(e.getMessage());
          }
        });
  }

  public static void record(String name, double value) {
    setGaugeMetrics(name + "_" + warehouseId, value);
    FirstExport(gaugeMetrics, MetricsType.GAUGE);
  }

  public static void increment(String name, double value) {
    incCounterMetrics(name + "_" + warehouseId, value);
    FirstExport(counterMetrics, MetricsType.COUNTER);
  }
}
