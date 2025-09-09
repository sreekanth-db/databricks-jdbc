package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.impl.DatabricksGeography;
import com.databricks.jdbc.api.impl.DatabricksGeometry;
import com.databricks.jdbc.api.impl.DatabricksGeospatial;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import org.apache.arrow.vector.util.Text;

public class GeospatialConverter implements ObjectConverter {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(GeospatialConverter.class);

  @Override
  public DatabricksGeometry toDatabricksGeometry(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksGeometry) {
      return (DatabricksGeometry) object;
    }

    if (object instanceof String || object instanceof Text) {
      String ewktString = object.toString();
      try {
        int srid = WKTConverter.extractSRIDFromEWKT(ewktString);
        String cleanWKT = WKTConverter.removeSRIDFromEWKT(ewktString);
        return new DatabricksGeometry(cleanWKT, srid);
      } catch (Exception e) {
        String errorMessage = String.format("Failed to convert EWKT to geometry: %s", ewktString);
        LOGGER.warn(errorMessage, e);
        throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
      }
    }

    throw new DatabricksSQLException(
        "Unsupported Geometry conversion from type: " + object.getClass(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  @Override
  public DatabricksGeography toDatabricksGeography(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksGeography) {
      return (DatabricksGeography) object;
    }

    if (object instanceof String || object instanceof Text) {
      String ewktString = object.toString();
      try {
        int srid = WKTConverter.extractSRIDFromEWKT(ewktString);
        String cleanWKT = WKTConverter.removeSRIDFromEWKT(ewktString);
        return new DatabricksGeography(cleanWKT, srid);
      } catch (Exception e) {
        String errorMessage = String.format("Failed to convert EWKT to geography: %s", ewktString);
        LOGGER.warn(errorMessage, e);
        throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
      }
    }

    throw new DatabricksSQLException(
        "Unsupported Geography conversion from type: " + object.getClass(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    if (object != null) {
      return object.toString();
    }
    throw new DatabricksSQLException(
        "Cannot convert null to String", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksGeospatial) {
      return ((DatabricksGeospatial) object).getWkb();
    }
    throw new DatabricksSQLException(
        "Unsupported byte array conversion operation for geospatial types",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }
}
