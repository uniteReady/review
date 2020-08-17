package com.tianya.bigdata.homework.day20200812;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogTest {

    public static void main(String[] args) {
        /*String log = "[18/Jan/2019:06:55:54 +0800]\t106.81.226.10\t-\t1410\t-\tGET\thttp://tianyafu5\t404\t4311\t2657\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";

        try {
            Access access = LogParser.parseLog(log);
            System.out.println(access);
        } catch (ParseException e) {
            e.printStackTrace();
        }*/
        Date date = MockAccessLog.randomDate("2019-01-01", "2019-01-02");
        System.out.println(date);
    }
}
