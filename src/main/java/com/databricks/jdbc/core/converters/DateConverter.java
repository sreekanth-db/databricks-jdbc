package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DateConverter extends AbstractObjectConverter {

    private Date object;
    public DateConverter(Object object) throws DatabricksSQLException {
        super(object);
        this.object = (Date) object;
    }

    @Override
    public Date convertToDate() throws DatabricksSQLException {
        return this.object;
    }

    @Override
    public short convertToShort() throws DatabricksSQLException {
        LocalDate localStartDate = LocalDate.ofEpochDay(0);
        long epochDays = ChronoUnit.DAYS.between(localStartDate, this.object.toLocalDate());
        if((short) epochDays == epochDays) {
            return (short) epochDays;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public int convertToInt() throws DatabricksSQLException {
        LocalDate localStartDate = LocalDate.ofEpochDay(0);
        long epochDays = ChronoUnit.DAYS.between(localStartDate, this.object.toLocalDate());
        if((int) epochDays == epochDays) {
            return (int) epochDays;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public long convertToLong() throws DatabricksSQLException {
        LocalDate localStartDate = LocalDate.ofEpochDay(0);
        return ChronoUnit.DAYS.between(localStartDate, this.object.toLocalDate());
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return this.object.toString();
    }

    @Override
    public Timestamp convertToTimestamp() throws DatabricksSQLException {
        return Timestamp.valueOf(this.object.toLocalDate().atStartOfDay());
    }
}
