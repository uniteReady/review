package com.tianya.bigdata.homework.day20200812.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    public static String[] getDateStrings(String time) throws Exception{
//        2020-08-14 11:31:10

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        Date timeParse = simpleDateFormat.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timeParse);
        String year = String.valueOf(calendar.get(Calendar.YEAR));
        String month = String.valueOf(calendar.get(Calendar.MONTH));
        String day = String.valueOf(calendar.get(Calendar.DAY_OF_MONTH));
        month = Integer.parseInt(month) < 10 ? "0" + month : month;
        day = Integer.parseInt(day) < 10 ? "0" + day : day;

        String[] dateStrings = {year,month,day};

        return dateStrings;
    }


    public static void main(String[] args) throws Exception {
//        String[] dateStrings = getDateStrings("2020-08-14 11:31:10");
//        System.out.println(dateStrings);
        String timeStr1= LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("当前时间为:"+timeStr1);


    }

}
