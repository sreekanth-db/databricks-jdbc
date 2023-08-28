package com.databricks.jdbc.core;

import org.apache.arrow.vector.types.Types;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.SQLException;

public class ArrowToJavaObjectConverter {
    public static Object convert(Object object, ColumnInfoTypeName requiredType, Types.MinorType arrowType) throws SQLException, IOException {
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
            case STRING:
                return convertToString(object, arrowType);
            default:
                throw new SQLException("Unsupported type");
        }
    }

    private static String convertToString(Object object, Types.MinorType arrowType) {
        return object.toString();
    }

    private static boolean convertToBoolean(Object object, Types.MinorType arrowType) {
        return (boolean) object;
    }

    private static byte[] convertToByteArray(Object object, Types.MinorType arrowType) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(object);
        return byteArrayOutputStream.toByteArray();
    }

    private static byte convertToByte(Object object, Types.MinorType arrowType) {
        return (byte) object;
    }

    private static short convertToShort(Object object, Types.MinorType arrowType) {
        return (short) object;
    }

    private static BigDecimal convertToBigDecimal(Object object, Types.MinorType arrowType) {
        return (BigDecimal) object;
    }

    private static double convertToDouble(Object object, Types.MinorType arrowType) {
        return (double) object;
    }

    private static float convertToFloat(Object object, Types.MinorType arrowType) {
        return (float) object;
    }

    private static int convertToInteger(Object object, Types.MinorType arrowType) {
        return (int) object;
    }

    private static long convertToLong(Object object, Types.MinorType arrowType) {
        return (long) object;
    }
}
