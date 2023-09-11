package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import org.junit.jupiter.api.Test;


import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

public class DateConverterTest {

    private Date DATE = Date.valueOf("2023-09-10");

    @Test
    public void testConvertToShort() throws DatabricksSQLException {
        assertEquals(new DateConverter(DATE).convertToShort(), 19610);

        Date dateDoesNotFitInShort = Date.valueOf("5050-12-31");
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new DateConverter(dateDoesNotFitInShort).convertToShort());
        assertTrue(exception.getMessage().contains("Invalid conversion"));
    }

    @Test
    public void testConvertToInt() throws DatabricksSQLException {
        assertEquals(new DateConverter(DATE).convertToInt(), 19610);
    }

    @Test
    public void testConvertToLong() throws DatabricksSQLException {
        assertEquals(new DateConverter(DATE).convertToLong(), 19610L);
    }
    //
    @Test
    public void testConvertToString() throws DatabricksSQLException {
        assertEquals(new DateConverter(DATE).convertToString(), "2023-09-10");
    }

    @Test
    public void testConvertToTimestamp() throws DatabricksSQLException {
        assertEquals(new DateConverter(DATE).convertToDate(), Timestamp.valueOf("2023-09-10 00:00:00"));
    }
}
