package com.databricks.jdbc.core.converters;

import java.math.BigDecimal;
import java.math.MathContext;

public class LongConverter extends AbstractObjectConverter {

    private int object;
    LongConverter(Object object, int columnTypeName) throws Exception {
        super(object, columnTypeName);
        this.object = (int) object;
    }

    @Override
    public boolean convertToBoolean() throws Exception {
        return (this.object == 0 ? true : false);
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
        short shortObject = (short) this.object;
        if(shortObject == this.object) {
            return shortObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public int convertToInt() throws Exception {
        int intObject = (int) this.object;
        if(intObject == this.object) {
            return intObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public float convertToFloat() throws Exception {
        float floatObject = (float) this.object;
        if(new BigDecimal(this.object).compareTo(new BigDecimal(floatObject, MathContext.DECIMAL32)) == 0) {
            return floatObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public double convertToDouble() throws Exception {
        return (double) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws Exception {
        return BigDecimal.valueOf(this.object);
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
