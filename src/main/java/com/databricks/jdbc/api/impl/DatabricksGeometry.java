package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksValidationException;

public class DatabricksGeometry extends AbstractDatabricksGeospatial {

  /**
   * Constructs a DatabricksGeometry with the specified WKT and SRID.
   *
   * @param wkt the Well-Known Text representation of the geometry
   * @param srid the Spatial Reference System Identifier
   * @throws DatabricksValidationException if the WKT is invalid
   */
  public DatabricksGeometry(String wkt, int srid) throws DatabricksValidationException {
    super(wkt, srid);
  }
}
