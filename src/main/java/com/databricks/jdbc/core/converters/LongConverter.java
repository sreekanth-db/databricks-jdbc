package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;

public class LongConverter extends AbstractObjectConverter {

    private long object;
    public LongConverter(Object object) throws DatabricksSQLException {
        super(object);
        this.object = (long) object;
    }

    @Override
    public long convertToLong() throws DatabricksSQLException {
        return this.object;
    }

    @Override
    public boolean convertToBoolean() throws DatabricksSQLException {
        return (this.object == 0 ? false : true);
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
        short shortObject = (short) this.object;
        if(shortObject == this.object) {
            return shortObject;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public int convertToInt() throws DatabricksSQLException {
        int intObject = (int) this.object;
        if(intObject == this.object) {
            return intObject;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public float convertToFloat() throws DatabricksSQLException {
        float floatObject = (float) this.object;
        if(new BigDecimal(this.object).compareTo(new BigDecimal(floatObject, MathContext.DECIMAL32)) == 0) {
            return floatObject;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public double convertToDouble() throws DatabricksSQLException {
        return (double) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
        return BigDecimal.valueOf(this.object);
    }

    @Override
    public byte[] convertToByteArray() throws DatabricksSQLException {
        return ByteBuffer.allocate(8).putLong(this.object).array();
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return String.valueOf(this.object);
    }
}
