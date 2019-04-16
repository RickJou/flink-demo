package com.realtime.app.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    private static ThreadLocal<DateFormat> SDF_YYYY_MM_DD_HH_MM_SS = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static ThreadLocal<DateFormat> SDF_YYYY_MM_DD = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    public static String getDate(Date date){
        return SDF_YYYY_MM_DD.get().format(date);
    }

    public static Long getTime(String strDate) throws ParseException {
        return SDF_YYYY_MM_DD_HH_MM_SS.get().parse(strDate).getTime();
    }

    public static Date parseYMDHSM(String strDate) {
        try {
            return SDF_YYYY_MM_DD_HH_MM_SS.get().parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取UTC-8时间戳(flink中采用的是utc时间,且时间窗口不支持offset负值(减少offset),相当于window.start和window.end时间都增加了8个小时,所以对于数据来说也要增加8个小时才能落到正确的窗口中)
     * @param datetime
     * @return
     * @throws ParseException
     */
    public static Long getUtcAdd8HourTime(String datetime) throws ParseException {
        Date date = SDF_YYYY_MM_DD_HH_MM_SS.get().parse(datetime);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.HOUR,8);
        return c.getTime().getTime();
    }

}
