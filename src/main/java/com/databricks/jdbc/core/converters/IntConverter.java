package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

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
    public long convertToLong() throws DatabricksSQLException {
        return (long) this.object;
    }

    @Override
    public float convertToFloat() throws DatabricksSQLException {
        return (float) this.object;
    }

    @Override
    public float convertToFloat(int scale) throws DatabricksSQLException {
        return convertToFloat()/super.POWERS_OF_TEN[scale];
    }

    @Override
    public double convertToDouble() throws DatabricksSQLException {
        return (double) this.object;
    }

    @Override
    public double convertToDouble(int scale) throws DatabricksSQLException {
        return convertToDouble()/super.POWERS_OF_TEN[scale];
    }

    @Override
    public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
        return BigDecimal.valueOf((long) this.object);
    }

    @Override
    public BigDecimal convertToBigDecimal(int scale) throws DatabricksSQLException {
        return BigDecimal.valueOf(convertToDouble()/super.POWERS_OF_TEN[scale]);
    }

    @Override
    public byte[] convertToByteArray() throws DatabricksSQLException {
        byte byteObject = (byte) this.object;
        if(byteObject == this.object) {
            return new byte[]{byteObject};
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return String.valueOf(this.object);
    }

    @Override
    public Date convertToDate() throws DatabricksSQLException {
        LocalDate localDate = LocalDate.ofEpochDay(this.object);
        return Date.valueOf(localDate);
    }

    @Override
    public Timestamp convertToTimestamp() throws DatabricksSQLException {
        return convertToTimestamp(super.DEFAULT_TIMESTAMP_SCALE);
    }

    @Override
    public Timestamp convertToTimestamp(int scale) throws DatabricksSQLException {
        long nanoseconds = (long) this.object * super.POWERS_OF_TEN[9 - scale];
        Time time = new Time(nanoseconds/super.POWERS_OF_TEN[6]);
        return new Timestamp(time.getTime());
    }
}
