package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class VolumeOperationExecutor implements Runnable {

    private static final String COMMA_SEPARATOR = ",";
    private static final String PARENT_DIRECTORY_REF = "..";
    private static final String GET_OPERATION = "get";
    private static final String PUT_OPERATION = "put";

    private static final Long PUT_SIZE_LIMITS = 5 * 1024 * 1024 * 1024L; // 5GB
    private final String operationType;
    private final String operationUrl;
    private final String localFilePath;
    private final ImmutableMap<String, String> headers;
    private final Set<String> allowedVolumeIngestionPaths;
    private VolumeOperationStatus status;
    private final IDatabricksHttpClient databricksHttpClient;
    private String errorMessage;

    VolumeOperationExecutor(
            String operationType,
            String operationUrl,
            String localFilePath,
            ImmutableMap<String, String> headers,
            String allowedVolumeIngestionPathString,
            IDatabricksHttpClient databricksHttpClient) {
        this.operationType = operationType;
        this.operationUrl = operationUrl;
        this.localFilePath = localFilePath;
        this.headers = headers;
        this.allowedVolumeIngestionPaths = new HashSet<>(
                Arrays.asList(allowedVolumeIngestionPathString.split(COMMA_SEPARATOR)));
        this.databricksHttpClient = databricksHttpClient;
        this.status = VolumeOperationStatus.PENDING;
        this.errorMessage = null;

    }

    @Override
    public void run() {
        validateLocalFilePath();
        if (status == VolumeOperationStatus.ABORTED) {
            return;
        }

    }

    private void validateLocalFilePath() {
        if (operationType.equalsIgnoreCase(DELETE_OPERATION)) {
            return;
        }
        if (localFilePath == null || localFilePath.isEmpty()) {
            status = VolumeOperationStatus.ABORTED;
            errorMessage = "Local file path is invalid";
            return;
        }
        if (!allowedVolumeIngestionPaths.contains(localFilePath)) {
            status = VolumeOperationStatus.ABORTED;
            errorMessage = "Local file path is not allowed";
            return;
        }
        if (localFilePath.contains(PARENT_DIRECTORY_REF)) {
            status = VolumeOperationStatus.ABORTED;
            errorMessage = "Local file path contains parent directory reference";
            return;
        }
    }

    private void executeGetOperation() {
        HttpGet httpGet = new HttpGet(operationUrl);
        headers.forEach(httpGet::addHeader);
        try (CloseableHttpResponse response = databricksHttpClient.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                status = VolumeOperationStatus.FAILED;
                errorMessage = "Failed to download file";
                return;
            }
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                // Get the content of the HttpEntity
                InputStream inputStream = entity.getContent();
                // Create a FileOutputStream to write the content to a file
                try (FileOutputStream outputStream = new FileOutputStream(new File(localFilePath))) {
                    // Copy the content of the InputStream to the FileOutputStream
                    byte[] buffer = new byte[1024];
                    int length;
                    while ((length = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, length);
                    }
                    status = VolumeOperationStatus.SUCCEEDED;
                } catch (FileNotFoundException e) {
                    status = VolumeOperationStatus.FAILED;
                    errorMessage = "Local file path is invalid or a directory";
                } catch (IOException e) {
                    // TODO: handle retries
                    status = VolumeOperationStatus.FAILED;
                    errorMessage = "Failed to write to local file: " + e.getMessage();
                } finally {
                    // It's important to consume the entity content fully and ensure the stream is closed
                    EntityUtils.consume(entity);
                }
            }
        } catch (IOException | DatabricksHttpException e) {
            status = VolumeOperationStatus.FAILED;
            errorMessage = "Failed to download file: " + e.getMessage();
        }
    }

    private void executePutOperation() {
        HttpPut httpPut = new HttpPut(operationUrl);
        headers.forEach(httpPut::addHeader);

        // Set the FileEntity as the request body
        File file = new File(localFilePath);
        if (!file.exists() || file.isDirectory()) {
            status = VolumeOperationStatus.ABORTED;
            errorMessage = "Local file does not exist or is a directory";
            return;
        }
        if (file.length() > 0) {
            status = VolumeOperationStatus.ABORTED;
            errorMessage = "Local file is empty";
            return;
        }

        FileEntity fileEntity = new FileEntity(file, ContentType.DEFAULT_BINARY);
        httpPut.setEntity(fileEntity);

        // Execute the request
        try (CloseableHttpResponse response = databricksHttpClient.execute(httpPut)) {
            // Process the response
            int statusCode = response.getStatusLine().getStatusCode();
            String statusLine = response.getStatusLine().toString();
            if (statusCode >= 200 && statusCode < 300) {
                status = VolumeOperationStatus.SUCCEEDED;
                return;
            }
        } catch (IOException | DatabricksHttpException e) {
            status = VolumeOperationStatus.FAILED;
            errorMessage = "Failed to upload file: " + e.getMessage();
        }
    }

    private void executeDeleteOperation() {
        HttpGet httpGet = new HttpGet(operationUrl);
        headers.forEach(httpGet::addHeader);
        try (CloseableHttpResponse response = databricksHttpClient.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                status = VolumeOperationStatus.FAILED;
                errorMessage = "Failed to download file";
                return;
            }
    }

    static enum VolumeOperationStatus {
        PENDING,
        RUNNING,
        ABORTED,
        SUCCEEDED,
        FAILED;
    }
}
