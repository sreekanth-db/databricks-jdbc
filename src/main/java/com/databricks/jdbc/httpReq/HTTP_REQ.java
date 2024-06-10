package com.databricks.jdbc.httpReq;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.utils.URIBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

public class HTTP_REQ {
    public static String sendPostRequest(String urlString, String accessToken, HashMap<String, Double> map) {
        StringBuilder result = new StringBuilder();
        try {
            // Convert the HashMap to JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonInputString = objectMapper.writeValueAsString(map);

            // Build the URL with the query parameter
            URIBuilder uriBuilder = new URIBuilder(urlString);
            uriBuilder.addParameter("metrics_map_string", jsonInputString);
            URL url = uriBuilder.build().toURL();

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Bearer " + accessToken);

            // Read response
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            String line;
            while ((line = br.readLine()) != null) {
                result.append(line);
            }
            br.close();
            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.toString();
    }

    public static void main(String[] args) {
        // Example usage
        String url = "https://test-shard-bhuvan-v2.dev.azuredatabricks.net/api/2.0/example-v2/exportMetrics";
        String accessToken = "dapif4da29d88f5a3f56a6654e3c46413dc2";
        HashMap<String, Double> map = new HashMap<>();
        map.put("key1", 1.0);
        map.put("key2", 2.0);
        String response = sendPostRequest(url, accessToken, map);
        System.out.println(response);
    }
}