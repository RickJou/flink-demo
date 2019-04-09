package com.realtime.app;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateUtil {

    private static ThreadLocal<DateFormat> SDF_YYYY_MM_DD_HH_MM_SS = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public static Long getTime(String strDate) throws ParseException {
        return SDF_YYYY_MM_DD_HH_MM_SS.get().parse(strDate).getTime();
    }
}
