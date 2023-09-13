package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class IntConverter extends AbstractObjectConverter {

    private int object;
    public IntConverter(Object object) throws DatabricksSQLException {
        super(object);
        this.object = (int) object;
    }

    @Override
    public int convertToInt() throws DatabricksSQLException {
        return this.object;
    }

    @Override
    public boolean convertToBoolean() throws DatabricksSQLException {
        return (this.object == 0 ? false : true);
    }

    @Override
    public byte convertToByte() throws DatabricksSQLException {
        if(this.object >= Byte.MIN_VALUE && this.object <= Byte.MAX_VALUE) {
            return (byte) this.object;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public short convertToShort() throws DatabricksSQLException {
        if(this.object >= Short.MIN_VALUE && this.object <= Short.MAX_VALUE) {
            return (short) this.object;
        }
        throw new DatabricksSQLException("Invalid conversion");
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
    public double convertToDouble() throws DatabricksSQLException {
        return (double) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
        return BigDecimal.valueOf((long) this.object);
    }

    @Override
    public byte[] convertToByteArray() throws DatabricksSQLException {
        return ByteBuffer.allocate(4).putInt(this.object).array();
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return String.valueOf(this.object);
    }
}
