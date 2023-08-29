package com.databricks.jdbc.core;

import org.apache.arrow.vector.types.Types;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;


import java.math.BigDecimal;
import java.time.ZoneId;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

public class ArrowToJavaObjectConverter {
    public static Object convert(Object object, ColumnInfoTypeName requiredType, Types.MinorType arrowType) throws SQLException {
        switch (requiredType) {
            case BYTE:
                return convertToByte(object, arrowType);
            case SHORT:
                return convertToShort(object, arrowType);
            case INT:
                return convertToInteger(object, arrowType);
            case LONG:
                return convertToLong(object, arrowType);
            case FLOAT:
                return convertToFloat(object, arrowType);
            case DOUBLE:
                return convertToDouble(object, arrowType);
            case DECIMAL:
                return convertToBigDecimal(object, arrowType);
            case BINARY:
                return convertToByteArray(object, arrowType);
            case BOOLEAN:
                return convertToBoolean(object, arrowType);
            case CHAR:
                return convertToChar(object, arrowType);
            case STRING:
                return convertToString(object, arrowType);
            case DATE:
                return convertToDate(object, arrowType);
            case TIMESTAMP:
                return convertToTimestamp(object, arrowType);
            case STRUCT:
                return convertToStringifiedMap(object, arrowType);
            case ARRAY:
                return convertToStringifiedArray(object, arrowType);
            case NULL:
                return null;
            default:
                throw new SQLException("Unsupported type");
        }
    }

    private static String convertToStringifiedArray(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.VARCHAR)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return object.toString();
    }

    private static String convertToStringifiedMap(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.VARCHAR)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return object.toString();
    }

    private static Object convertToTimestamp(Object object, Types.MinorType arrowType) throws SQLException {
        if(!(arrowType.equals(Types.MinorType.INT) || arrowType.equals(Types.MinorType.BIGINT))) {
            throw new SQLException("Data does not agree with metadata");
        }
        Instant instant = arrowType == Types.MinorType.INT ?
                Instant.ofEpochMilli((int) object) : Instant.ofEpochMilli((long) object);
        return Timestamp.from(instant);
    }

    private static Date convertToDate(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.DATEDAY)) {
            throw new SQLException("Data does not agree with metadata");
        }
        LocalDate localDate = LocalDate.ofEpochDay((int) object);
        return Date.valueOf(localDate);
    }

    private static char convertToChar(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.VARCHAR)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (object.toString()).charAt(0);
    }

    private static String convertToString(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.VARCHAR)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return object.toString();
    }

    private static boolean convertToBoolean(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.BIT)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (boolean) object;
    }

    private static byte[] convertToByteArray(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.VARBINARY)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (byte[]) object;
    }

    private static byte convertToByte(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.TINYINT)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (byte) object;
    }

    private static short convertToShort(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.SMALLINT)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (short) object;
    }

    private static BigDecimal convertToBigDecimal(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.DECIMAL)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (BigDecimal) object;
    }

    private static double convertToDouble(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.FLOAT8)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (double) object;
    }

    private static float convertToFloat(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.FLOAT4)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (float) object;
    }

    private static int convertToInteger(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.INT)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (int) object;
    }

    private static long convertToLong(Object object, Types.MinorType arrowType) throws SQLException {
        if(!arrowType.equals(Types.MinorType.BIGINT)) {
            throw new SQLException("Data does not agree with metadata");
        }
        return (long) object;
    }
}
