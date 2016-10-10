package com.unixera.storm.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

/**
 * Created by Frost Wong on 10/10/16.
 */
public class DateTime {
    private Calendar calendar;
    private Integer year;
    private Integer month;
    private Integer dayOfMonth;
    private Integer hour;

    public DateTime(String localTime) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZ", Locale.ENGLISH);

            calendar = Calendar.getInstance();
            calendar.setTime(simpleDateFormat.parse(localTime));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public String getYear() {
        year = calendar.get(Calendar.YEAR);
        return year.toString();
    }


    public String getMonth() {
        month = calendar.get(Calendar.MONTH) + 1;
        if (month < 10) {
            return "0" + month.toString();
        }
        return month.toString();
    }

    public String getDayOfMonth() {
        dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
        if (dayOfMonth > 10) {
            return dayOfMonth.toString();
        }
        return "0" + dayOfMonth.toString();
    }

    public String getDayWithYearAndMonth() {
        return getYear() + getMonth() + getDayOfMonth();
    }

    public String getHour() {
        hour = calendar.get(Calendar.HOUR_OF_DAY);
        if (hour >= 10) {
            return hour.toString();
        }
        return "0" + hour.toString();
    }
}
