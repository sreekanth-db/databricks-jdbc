package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksValidationException;

/**
 * Interface for geospatial data types in Databricks JDBC driver.
 *
 * <p>This interface provides common functionality for both GEOMETRY and GEOGRAPHY types, allowing
 * access to Well-Known Text (WKT), Well-Known Binary (WKB) representation and Spatial Reference
 * System Identifier (SRID).
 *
 * <p>Following the established patterns of DatabricksStruct, DatabricksArray, and DatabricksMap,
 * this interface enables consistent handling of geospatial data across the JDBC driver.
 */
public interface IDatabricksGeospatial {

  /**
   * Returns the Well-Known Binary (WKB) representation of the geospatial object.
   *
   * <p>WKB is a binary format for representing geometry data that is compact and suitable for
   * storage and transmission. This method converts the internal representation to WKB format on
   * demand.
   *
   * @return the WKB representation as a byte array
   * @throws DatabricksValidationException if WKT to WKB conversion fails
   */
  byte[] getWkb() throws DatabricksValidationException;

  /**
   * Returns the Spatial Reference System Identifier (SRID) of the geospatial object.
   *
   * <p>SRID identifies the coordinate system used by the geometry. Common values include:
   *
   * <ul>
   *   <li>4326 - WGS 84 (World Geodetic System 1984)
   *   <li>3857 - Web Mercator
   *   <li>0 - No SRID specified
   * </ul>
   *
   * @return the SRID value
   */
  int getSrid();

  /**
   * Returns the Well-Known Text (WKT) representation of the geospatial object.
   *
   * <p>WKT is a human-readable text format for representing geometry data. This provides a
   * complement to the binary WKB format, allowing easy inspection and debugging of geospatial data.
   *
   * @return the WKT string representation
   */
  String getWkt();
}
