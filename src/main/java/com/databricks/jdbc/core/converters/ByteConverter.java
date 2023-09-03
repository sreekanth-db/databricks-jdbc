package com.databricks.jdbc.core.converters;

import java.math.BigDecimal;

public class ByteConverter extends AbstractObjectConverter {

    private byte object;
    ByteConverter(Object object, int columnTypeName) throws Exception {
        super(object, columnTypeName);
        this.object = (byte) object;
    }

    @Override
    public boolean convertToBoolean() throws Exception {
        return (this.object == 0 ? true : false);
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
    public float convertToFloat() throws Exception {
        return (float) this.object;
    }

    @Override
    public double convertToDouble() throws Exception {
        return (double) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws Exception {
        return BigDecimal.valueOf((long) this.object);
    }

    @Override
    public byte[] convertToByteArray() throws Exception {
        return new byte[]{this.object};
    }

    @Override
    public String convertToString() throws Exception {
        return new String(new byte[]{this.object});
    }
}
