package com.databricks.jdbc.core.converters;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Date;

public abstract class AbstractObjectConverter {
    Object object;
    int columnTypeName;
    AbstractObjectConverter(Object object, int columnTypeName) throws Exception {
        this.object = object;
        this.columnTypeName = columnTypeName;
    }

    public byte convertToByte() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public short convertToShort() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public int convertToInt() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public long convertToLong() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public float convertToFloat() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public double convertToDouble() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public BigDecimal convertToBigDecimal() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public byte[] convertToByteArray() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public char convertToChar() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public String convertToString() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public Timestamp convertToTimestamp() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public Date convertToDate() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public String convertToStruct() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }

    public String convertToArray() throws Exception {
        throw new Exception("Unsupported conversion operation");
    }
}
