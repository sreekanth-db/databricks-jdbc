package com.databricks.jdbc.core.converters;

import java.math.BigDecimal;

public class BooleanConverter extends AbstractObjectConverter {

    private Boolean object;
    BooleanConverter(Object object, int columnTypeName) throws Exception {
        super(object, columnTypeName);
        this.object = (boolean) object;
    }

    @Override
    public byte convertToByte() throws Exception {
        return (byte) (this.object ? 1 : 0);
    }

    @Override
    public short convertToShort() throws Exception {
        return (short) (this.object ? 1 : 0);
    }

    @Override
    public int convertToInt() throws Exception {
        return this.object ? 1 : 0;
    }

    @Override
    public long convertToLong() throws Exception {
        return this.object ? 1L : 0L;
    }

    @Override
    public float convertToFloat() throws Exception {
        return this.object ? 1f : 0f;
    }

    @Override
    public double convertToDouble() throws Exception {
        return (double) (this.object ? 1 : 0);
    }

    @Override
    public BigDecimal convertToBigDecimal() throws Exception {
        return BigDecimal.valueOf(this.object ? 1 : 0);
    }

    @Override
    public byte[] convertToByteArray() throws Exception {
        return this.object ? new byte[]{1} : new byte[]{0};
    }

    @Override
    public char convertToChar() throws Exception {
        return this.object ? '1' : '0';
    }

    @Override
    public String convertToString() throws Exception {
        return this.object ? "1" : "0";
    }
}
