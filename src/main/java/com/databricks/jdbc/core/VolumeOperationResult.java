package com.databricks.jdbc.core;

import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;

import java.sql.SQLException;

class VolumeOperationResult implements IExecutionResult {

    private final IDatabricksSession session;
    private final ResultManifest resultManifest;
    private final ResultData resultData;
    private final String statementId;
    private VolumeOperationExecutor volumeOperationExecutor;

    VolumeOperationResult(ResultManifest resultManifest, ResultData resultData, String statementId, IDatabricksSession session) throws DatabricksSQLException {
        this.resultManifest = resultManifest;
        this.resultData = resultData;
        this.statementId = statementId;
        this.session = session;
        init();
    }

    void init() throws DatabricksSQLException {
        if (!session.getClientInfoProperties().containsKey(DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase())) {
            throw new DatabricksSQLException("UC Volume Operation is not allowed");
        }
        this.volumeOperationExecutor = new VolumeOperationExecutor(null, null, null, null, session.getClientInfoProperties().get(DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase()));
        Thread thread = new Thread(volumeOperationExecutor);
        thread.setDaemon(true);
        thread.setName("VolumeOperationExecutor " + statementId);
        thread.start();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public long getCurrentRow() {
        return 0;
    }

    @Override
    public boolean next() throws DatabricksSQLException {
        return false;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void close() {

    }
}
