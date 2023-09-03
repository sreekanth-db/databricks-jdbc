package com.databricks.jdbc.core.converters;

import java.math.BigDecimal;
import java.math.MathContext;

public class FloatConverter extends AbstractObjectConverter {

    private float object;
    FloatConverter(Object object, int columnTypeName) throws Exception {
        super(object, columnTypeName);
        this.object = (float) object;
    }

    @Override
    public boolean convertToBoolean() throws Exception {
        return (this.object == 0f ? true : false);
    }

    @Override
    public byte convertToByte() throws Exception {
        byte byteObject = (byte) this.object;
        if(byteObject == this.object) {
            return byteObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public short convertToShort() throws Exception {
        return (short) this.object;
    }

    @Override
    public int convertToInt() throws Exception {
        return (int) this.object;
    }

    @Override
    public long convertToLong() throws Exception {
        return (long) this.object;
    }

    @Override
    public double convertToDouble() throws Exception {
        return (double) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws Exception {
        return new BigDecimal(Float.toString(this.object));
    }

    @Override
    public byte[] convertToByteArray() throws Exception {
        byte byteObject = (byte) this.object;
        if(byteObject == this.object) {
            return new byte[]{byteObject};
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public String convertToString() throws Exception {
        return String.valueOf(this.object);
    }
}
