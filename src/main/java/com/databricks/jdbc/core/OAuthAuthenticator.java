package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;

public class OAuthAuthenticator {
    public OAuthAuthenticator() {
    }

    public WorkspaceClient authenticateU2M(DatabricksConnectionContext connectionContext) {
        DatabricksConfig config = new DatabricksConfig()
                .setAuthType("databricks-cli")
                .setHost(connectionContext.getHostUrl())
                .setClientId("placeholder")
                .setClientSecret("placeholder");
        return new WorkspaceClient(config);
    }

    public WorkspaceClient authenticatePersonalAccessToken(DatabricksConnectionContext connectionContext) {
        DatabricksConfig config = new DatabricksConfig()
                .setAuthType("pat")
                .setHost(connectionContext.getHostUrl())
                .setToken(connectionContext.getToken());
        return new WorkspaceClient(config);
    }

    public WorkspaceClient authenticateM2M(DatabricksConnectionContext connectionContext) {
        DatabricksConfig config = new DatabricksConfig()
                .setAuthType("oauth-m2m")
                .setHost(connectionContext.getHostUrl())
                .setClientId("placeholder")
                .setClientSecret("placeholder");
        return new WorkspaceClient(config);
    }

}
