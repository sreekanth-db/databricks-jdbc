package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksValidationException;
import org.junit.jupiter.api.Test;

/** Test class for WKTConverter utility. */
public class WKTConverterTest {

  @Test
  public void testToWKB_ValidWKT() throws DatabricksValidationException {
    String wkt = "POINT(1 2)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);
    // WKB is binary data, not UTF-8 bytes of WKT
    // We can verify it's valid WKB by converting it back to WKT
    String convertedBack = WKTConverter.toWKT(wkb);
    // JTS outputs "POINT (1 2)" with a space after POINT, which is valid WKT
    assertEquals("POINT (1 2)", convertedBack);
  }

  @Test
  public void testToWKB_NullWKT() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB(null));
  }

  @Test
  public void testToWKB_EmptyWKT() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB(""));
  }

  @Test
  public void testToWKB_WhitespaceWKT() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB("   "));
  }

  @Test
  public void testExtractSRIDFromEWKT_WithSRID() {
    String ewkt = "SRID=4326;POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(ewkt);
    assertEquals(4326, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_WithoutSRID() {
    String wkt = "POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(wkt);
    assertEquals(0, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_Null() {
    int srid = WKTConverter.extractSRIDFromEWKT(null);
    assertEquals(0, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_Empty() {
    int srid = WKTConverter.extractSRIDFromEWKT("");
    assertEquals(0, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_InvalidSRID() {
    String ewkt = "SRID=invalid;POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(ewkt);
    assertEquals(0, srid); // Should return 0 for invalid SRID
  }

  @Test
  public void testExtractSRIDFromEWKT_NoSemicolon() {
    String ewkt = "SRID=4326POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(ewkt);
    assertEquals(0, srid); // Should return 0 if no semicolon
  }

  @Test
  public void testRemoveSRIDFromEWKT_WithSRID() {
    String ewkt = "SRID=4326;POINT(1 2)";
    String wkt = WKTConverter.removeSRIDFromEWKT(ewkt);
    assertEquals("POINT(1 2)", wkt);
  }

  @Test
  public void testRemoveSRIDFromEWKT_WithoutSRID() {
    String wkt = "POINT(1 2)";
    String result = WKTConverter.removeSRIDFromEWKT(wkt);
    assertEquals("POINT(1 2)", result);
  }

  @Test
  public void testRemoveSRIDFromEWKT_Null() {
    String result = WKTConverter.removeSRIDFromEWKT(null);
    assertNull(result);
  }

  @Test
  public void testRemoveSRIDFromEWKT_Empty() {
    String result = WKTConverter.removeSRIDFromEWKT("");
    assertEquals("", result);
  }

  @Test
  public void testRemoveSRIDFromEWKT_NoSemicolon() {
    String ewkt = "SRID=4326POINT(1 2)";
    String result = WKTConverter.removeSRIDFromEWKT(ewkt);
    assertEquals("SRID=4326POINT(1 2)", result); // Should return as-is if no semicolon
  }

  @Test
  public void testRemoveSRIDFromEWKT_OnlySRID() {
    String ewkt = "SRID=4326;";
    String result = WKTConverter.removeSRIDFromEWKT(ewkt);
    assertEquals("", result);
  }
}
