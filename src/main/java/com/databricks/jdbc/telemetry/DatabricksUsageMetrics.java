package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

public class DatabricksUsageMetrics {
  private static final String URL =
      "https://aa87314c1e33d4c1f91a919f8cf9c4ba-387609431.us-west-2.elb.amazonaws.com:443/api/2.0/oss-sql-driver-telemetry/usageMetrics";
  private static String workspaceId = null;
  private static DatabricksHttpClient telemetryClient;

  private static void initialize(IDatabricksConnectionContext context)
      throws DatabricksSQLException {
    workspaceId = context.getComputeResource().getWorkspaceId();
    telemetryClient = DatabricksHttpClient.getInstance(context);
  }

  private static HttpUriRequest getRequest(
      String jvmName,
      String jvmSpecVersion,
      String jvmImplVersion,
      String jvmVendor,
      String osName,
      String osVersion,
      String osArch,
      String localeName,
      String charsetEncoding)
      throws Exception {
    URIBuilder uriBuilder = new URIBuilder(URL);
    uriBuilder.addParameter("workspace_id", workspaceId);
    uriBuilder.addParameter("jvm_name", jvmName);
    uriBuilder.addParameter("jvm_spec_version", jvmSpecVersion);
    uriBuilder.addParameter("jvm_impl_version", jvmImplVersion);
    uriBuilder.addParameter("jvm_vendor", jvmVendor);
    uriBuilder.addParameter("os_name", osName);
    uriBuilder.addParameter("os_version", osVersion);
    uriBuilder.addParameter("os_arch", osArch);
    uriBuilder.addParameter("locale_name", localeName);
    uriBuilder.addParameter("charset_encoding", charsetEncoding);
    return new HttpPost(uriBuilder.build());
  }

  public static void exportUsageMetrics(
      IDatabricksConnectionContext context,
      String jvmName,
      String jvmSpecVersion,
      String jvmImplVersion,
      String jvmVendor,
      String osName,
      String osVersion,
      String osArch,
      String localeName,
      String charsetEncoding)
      throws DatabricksSQLException {
    initialize(context);
    try {
      HttpUriRequest request =
          getRequest(
              jvmName,
              jvmSpecVersion,
              jvmImplVersion,
              jvmVendor,
              osName,
              osVersion,
              osArch,
              localeName,
              charsetEncoding);
      CloseableHttpResponse response = telemetryClient.executeWithoutSSL(request);

      if (response == null) {
        LoggingUtil.log(LogLevel.DEBUG, "Response is null for usage metrics export.");
      } else if (response.getStatusLine().getStatusCode() != 200) {
        LoggingUtil.log(
            LogLevel.DEBUG,
            "Response code for usage metrics export: "
                + response.getStatusLine().getStatusCode()
                + " Response: "
                + response.getEntity().toString());
      } else {
        LoggingUtil.log(LogLevel.DEBUG, EntityUtils.toString(response.getEntity()));
        response.close();
      }
    } catch (Exception e) {
      LoggingUtil.log(LogLevel.DEBUG, e.getMessage());
    }
  }
}
