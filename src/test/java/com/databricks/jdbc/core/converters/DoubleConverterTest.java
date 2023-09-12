package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class DoubleConverterTest {

    private double NON_ZERO_OBJECT = 10.2;
    private double ZERO_OBJECT = 0;
    @Test
    public void testConvertToByte() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToByte(), (byte) 0);
    }

    @Test
    public void testConvertToShort() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToShort(), (short) 0);
    }

    @Test
    public void testConvertToInt() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToInt(), (int) 0);
    }

    @Test
    public void testConvertToLong() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToLong(), 0L);
    }

    @Test
    public void testConvertToFloat() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToFloat(), 10.2f);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToFloat(), 0f);
    }

    @Test
    public void testConvertToDouble() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10.2);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
    }

    @Test
    public void testConvertToBigDecimal() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToBigDecimal(), new BigDecimal(Double.toString(10.2)));
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToBigDecimal(), new BigDecimal(Double.toString(0)));
    }

    @Test
    public void testConvertToBoolean() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToBoolean(), false);
    }

    @Test
    public void testConvertToByteArray() throws DatabricksSQLException {
        assertTrue(Arrays.equals(new DoubleConverter(NON_ZERO_OBJECT).convertToByteArray(),
                ByteBuffer.allocate(8).putDouble(10.2).array()));
        assertTrue(Arrays.equals(new DoubleConverter(ZERO_OBJECT).convertToByteArray(),
                ByteBuffer.allocate(8).putDouble(0).array()));
    }

    @Test
    public void testConvertToChar() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new DoubleConverter(NON_ZERO_OBJECT).convertToChar());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToString() throws DatabricksSQLException {
        assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToString(), "10.2");
        assertEquals(new DoubleConverter(ZERO_OBJECT).convertToString(), "0.0");
    }

    @Test
    public void testConvertToTimestamp() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new DoubleConverter(NON_ZERO_OBJECT).convertToTimestamp());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToDate() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new DoubleConverter(NON_ZERO_OBJECT).convertToDate());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }
}
