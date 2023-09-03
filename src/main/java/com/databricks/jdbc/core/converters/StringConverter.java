package com.databricks.jdbc.core.converters;

import java.math.BigDecimal;

public class StringConverter extends AbstractObjectConverter {

    private String object;
    StringConverter(Object object, int columnTypeName) throws Exception {
        super(object, columnTypeName);
        this.object = (String) object;
    }

    @Override
    public boolean convertToBoolean() throws Exception {
        if("0".equals(this.object) || "false".equalsIgnoreCase(this.object)) {
            return false;
        }
        else if("1".equals(this.object) || "true".equalsIgnoreCase(this.object)) {
            return true;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public byte convertToByte() throws Exception {
        return this.object.getBytes()[0];
    }

    @Override
    public short convertToShort() throws Exception {
        short shortObject = Short.parseShort(this.object);
        if(String.valueOf(shortObject) == this.object) {
            return shortObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public int convertToInt() throws Exception {
        int intObject = Integer.parseInt(this.object);
        if(String.valueOf(intObject) == this.object) {
            return intObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public long convertToLong() throws Exception {
        long longObject = Long.parseLong(this.object);
        if(String.valueOf(longObject) == this.object) {
            return longObject;
        }
        throw new Exception("Invalid conversion");
    }

    @Override
    public float convertToFloat() throws Exception {
        return Float.parseFloat(this.object);
    }

    @Override
    public double convertToDouble() throws Exception {
        return Double.parseDouble(this.object);
    }

    @Override
    public BigDecimal convertToBigDecimal() throws Exception {
        return BigDecimal.valueOf(Double.parseDouble(this.object));
    }

    @Override
    public byte[] convertToByteArray() throws Exception {
        return this.object.getBytes();
    }
}
