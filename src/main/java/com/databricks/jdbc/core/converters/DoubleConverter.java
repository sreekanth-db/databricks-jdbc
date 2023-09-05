package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;

public class DoubleConverter extends AbstractObjectConverter {

    private double object;
    public DoubleConverter(Object object) throws DatabricksSQLException {
        super(object);
        this.object = (double) object;
    }

    @Override
    public double convertToDouble() throws DatabricksSQLException {
        return this.object;
    }

    @Override
    public boolean convertToBoolean() throws DatabricksSQLException {
        return (this.object == 0f ? false : true);
    }

    @Override
    public byte convertToByte() throws DatabricksSQLException {
        byte byteObject = (byte) this.object;
        if(byteObject == this.object) {
            return byteObject;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public short convertToShort() throws DatabricksSQLException {
        return (short) this.object;
    }

    @Override
    public int convertToInt() throws DatabricksSQLException {
        return (int) this.object;
    }

    @Override
    public long convertToLong() throws DatabricksSQLException {
        return (long) this.object;
    }

    @Override
    public float convertToFloat() throws DatabricksSQLException {
        return (float) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
        return new BigDecimal(Double.toString(this.object));
    }

    @Override
    public byte[] convertToByteArray() throws DatabricksSQLException {
        return ByteBuffer.allocate(8).putDouble(this.object).array();
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return String.valueOf(this.object);
    }
}
