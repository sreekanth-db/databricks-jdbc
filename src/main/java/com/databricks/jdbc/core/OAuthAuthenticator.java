package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;

public class OAuthAuthenticator {

    private IDatabricksConnectionContext connectionContext;

    public OAuthAuthenticator(IDatabricksConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
    }

    public WorkspaceClient getWorkspaceClient() {
        if(this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.PAT)) {
            return authenticatePersonalAccessToken();
        }
        // TODO(Madhav): Revisit these to set JDBC values
        else if(this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.OAUTH)) {
            switch(this.connectionContext.getAuthFlow()) {
                case TOKEN_PASSTHROUGH:
                    return authenticateU2M();
                case CLIENT_CREDENTIALS:
                case BROWSER_BASED_AUTHENTICATION:
                    return authenticateM2M();
            }
        }
        return authenticatePersonalAccessToken();
    }

    public WorkspaceClient authenticateU2M() {
        DatabricksConfig config = new DatabricksConfig()
                .setAuthType("databricks-cli")
                .setHost(this.connectionContext.getHostUrl())
                .setClientId(this.connectionContext.getClientId())
                .setClientSecret(this.connectionContext.getClientSecret());
        return new WorkspaceClient(config);
    }

    public WorkspaceClient authenticatePersonalAccessToken() {
        DatabricksConfig config = new DatabricksConfig()
                .setAuthType("pat")
                .setHost(this.connectionContext.getHostUrl())
                .setToken(this.connectionContext.getToken());
        return new WorkspaceClient(config);
    }

    public WorkspaceClient authenticateM2M() {
        DatabricksConfig config = new DatabricksConfig()
                .setAuthType("oauth-m2m")
                .setHost(this.connectionContext.getHostUrl())
                .setClientId(this.connectionContext.getClientId())
                .setClientSecret(this.connectionContext.getClientSecret());
        return new WorkspaceClient(config);
    }
}
