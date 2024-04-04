package com.databricks.jdbc.client.impl.thrift.commons;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksThriftHelper.class);
  public static final List<TStatusCode> SUCCESS_STATUS_LIST =
      List.of(TStatusCode.SUCCESS_STATUS, TStatusCode.SUCCESS_WITH_INFO_STATUS);

  public static TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    ByteBuffer newBuffer = buffer.duplicate(); // This is to avoid a BufferUnderflowException
    long sigBits = newBuffer.getLong();
    return new UUID(sigBits, sigBits).toString();
  }

  public static void verifySuccessStatus(TStatusCode statusCode, String errorContext)
      throws DatabricksHttpException {
    if (!SUCCESS_STATUS_LIST.contains(statusCode)) {
      String errorMessage = "Error while receiving thrift response " + errorContext;
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(errorMessage);
    }
  }

  public static int getColumnCount(TGetResultSetMetadataResp resultManifest) {
    if (resultManifest == null || resultManifest.getSchema() == null) {
      return 0;
    }
    return resultManifest.getSchema().getColumnsSize();
  }

  public static int getRowCount(TRowSet resultData) {
    List<TColumn> columns = resultData.getColumns();
    if (columns == null || columns.isEmpty()) {
      return 0;
    }

    TColumn tColumn = columns.get(0);
    Function<TColumn, Integer> rowCountFunction =
        column -> {
          if (column.isSetBinaryVal()) return column.getBinaryVal().getValuesSize();
          if (column.isSetBoolVal()) return column.getBoolVal().getValuesSize();
          if (column.isSetByteVal()) return column.getByteVal().getValuesSize();
          if (column.isSetDoubleVal()) return column.getDoubleVal().getValuesSize();
          if (column.isSetI16Val()) return column.getI16Val().getValuesSize();
          if (column.isSetI32Val()) return column.getI32Val().getValuesSize();
          if (column.isSetI64Val()) return column.getI64Val().getValuesSize();
          return column.getStringVal().getValuesSize(); // Default case
        };

    return rowCountFunction.apply(tColumn);
  }
}
