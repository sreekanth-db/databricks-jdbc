package com.databricks.jdbc.metrics_telemetry;

import java.io.IOException;
import java.util.HashMap;

import com.databricks.jdbc.httpReq.HTTP_REQ;

public class MetricsMap {
    private static final String URL = "https://test-shard-bhuvan-v2.dev.azuredatabricks.net/api/2.0/example-v2/exportMetrics";
    private static final String ACCESS_TOKEN = "dapif4da29d88f5a3f56a6654e3c46413dc2";
    private static final HashMap<String, Double> gaugeMetrics = new HashMap<>();
    private static final HashMap<String, Double> counterMetrics = new HashMap<>();

    private static long LastModified = System.currentTimeMillis();

    private static int INTERVAL = 0;

    private static long count_http_post = 0;
    private MetricsMap() throws IOException {
        // Private constructor to prevent instantiation
    }

    public static void postMetrics() {
        long current_time = System.currentTimeMillis();

        if(current_time - LastModified >= INTERVAL) {
            LastModified = current_time;
            String response = HTTP_REQ.sendPostRequest(URL, ACCESS_TOKEN, gaugeMetrics);
            System.out.println("Response" + response);
            count_http_post+= (System.currentTimeMillis() - LastModified);
        }
    }

    public static void SetGaugeMetric(String name, double value) {
        if(!gaugeMetrics.containsKey(name)) {
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
