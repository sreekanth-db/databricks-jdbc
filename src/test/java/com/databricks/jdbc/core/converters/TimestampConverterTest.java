package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TimestampConverterTest {

    private Timestamp TIMESTAMP = Timestamp.valueOf("2023-09-10 20:45:00");


    @Test
    public void testConvertToLong() throws DatabricksSQLException {
        assertEquals(new TimestampConverter(TIMESTAMP).convertToLong(), 1694358900000L);
    }

    @Test
    public void testConvertToString() throws DatabricksSQLException {
        assertEquals(new TimestampConverter(TIMESTAMP).convertToString(), "2023-09-10 20:45:00.0");
    }

    @Test
    public void testConvertToDate() throws DatabricksSQLException {
        assertEquals(new TimestampConverter(TIMESTAMP).convertToDate(), Date.valueOf("2023-09-10"));
    }
}
