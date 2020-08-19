package com.tianya.bigdata.homework.day20200812;

import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;
import org.lionsoul.ip2region.Util;

import java.io.File;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class LogParser {

    public static final SimpleDateFormat FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);

    public static Access parseLog(DbSearcher searcher,String log) throws ParseException {
//        String log = "[29/Jan/2019:01:51:04 +0800]\t61.236.169.40\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";
//        String log = "[29/Jan/2019:01:51:04 +0800]\t61.235.87.102\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";

        Access access = new Access();
        String[] splits = log.split("\t");
        String time = splits[0];
        time = time.substring(1, time.length() - 1);
        Date date = FORMAT.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String ip = splits[1];
        String proxyIp = splits[2];
        String responseTime = splits[3];
        String referer = splits[4];
        String method = splits[5];
        String url = splits[6];
        String httpCode = splits[7];
        String requestSize = splits[8];
        String responseSize = splits[9];
        String cache = splits[10];
        String uaHead = splits[11];
        String type = splits[12];
        access.setIp(ip);
        access.setProxyIp(proxyIp);
        access.setResponseTime(Long.parseLong(responseTime));
        access.setReferer(referer);
        access.setMethod(method);
        access.setUrl(url);
        access.setHttpCode(httpCode);
        access.setRequestSize(Long.parseLong(requestSize));
        access.setCache(cache);
        access.setUaHead(uaHead);
        access.setType(type);

        Integer year = calendar.get(Calendar.YEAR);
        Integer month = calendar.get(Calendar.MONTH) + 1;
        Integer day = calendar.get(Calendar.DATE);
        access.setYear(year + "");
        access.setMonth(month < 10 ? "0" + month : month + "");
        access.setDay(day < 10 ? "0" + day : day + "");

        String ipInfo = IPUtils.getCityInfo(searcher, ip);
        String[] ipInfos = ipInfo.split("\\|");

        String province = ipInfos[2];
        String city = ipInfos[3];
        String isp = ipInfos[4];
        access.setProvince(province);
        access.setCity(city);
        access.setIsp(isp);

        String[] urlSplits = url.split("\\?");
        String[] urlSplits2 = urlSplits[0].split(":");

        String http = urlSplits2[0];
        access.setHttp(http);
        String urlSpliting = urlSplits2[1].substring(2);
        String domain = urlSpliting;
        String path = "";
        if (urlSpliting.contains("/")) {
            domain = urlSpliting.substring(0, urlSpliting.indexOf("/"));
            path = urlSpliting.substring(urlSpliting.indexOf("/"));
        }
        access.setDomain(domain);

        access.setPath(path);
        String params = urlSplits.length == 2 ? urlSplits[1] : null;
        access.setParams(params);
        access.setResponseSize(Long.parseLong(responseSize));
        return access;


    }

    public static void main(String[] args) throws ParseException {
        /*String log = "[29/Jan/2019:01:51:04 +0800]\t61.236.169.40\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";
        Access access = parseLog(log);
        System.out.println(access);*/



        /*String log = "[29/Jan/2019:01:51:04 +0800]\t61.236.169.40\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";
//        String log = "[29/Jan/2019:01:51:04 +0800]\t61.235.87.102\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";

        Access access = new Access();
        String[] splits = log.split("\t");
        String time = splits[0];
        time = time.substring(1,time.length()-1);
        Date date = FORMAT.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String ip = splits[1];
        String proxyIp = splits[2];
        String responseTime = splits[3];
        String referer = splits[4];
        String method = splits[5];
        String url = splits[6];
        String httpCode = splits[7];
        String requestSize = splits[8];
        String responseSize = splits[9];
        String cache = splits[10];
        String uaHead = splits[11];
        String type = splits[12];
        access.setIp(ip);
        access.setProxyIp(proxyIp);
        access.setResponseTime(Long.parseLong(responseTime));
        access.setReferer(referer);
        access.setMethod(method);
        access.setUrl(url);
        access.setHttpCode(httpCode);
        access.setRequestSize(Long.parseLong(requestSize));
//        access.setResponseSize(responseSize);
        access.setCache(cache);
        access.setUaHead(uaHead);
        access.setType(type);

        Integer year = calendar.get(Calendar.YEAR);
        Integer month = calendar.get(Calendar.MONTH) + 1;
        Integer day = calendar.get(Calendar.DATE);
        access.setYear(year + "");
        access.setMonth(month < 10 ? "0" + month : month + "");
        access.setDay(day < 10 ? "0" + day : day + "");

        String ipInfo = analizeIp(ip);
        String[] ipInfos = ipInfo.split("\\|");

        String province = ipInfos[2];
        String city = ipInfos[3] ;
        String isp = ipInfos[4];
        access.setProvince(province);
        access.setCity(city);
        access.setIsp(isp);

        String[] urlSplits = url.split("\\?");
        String[] urlSplits2 = urlSplits[0].split(":");

        String http = urlSplits2[0];
        access.setHttp(http);
        String urlSpliting = urlSplits2[1].substring(2);
        String domain=urlSpliting.substring(0,urlSpliting.indexOf("/"));
        access.setDomain(domain);

        String path=urlSpliting.substring(urlSpliting.indexOf("/"));
        access.setPath(path);
        String params = urlSplits.length==2?urlSplits[1]:null;
        access.setParams(params);

        System.out.println(access);*/


    }

    public static String analizeIp(String ip) {
        //db
        String dbPath = LogParser.class.getResource("/ip2region.db").getPath();

        File file = new File(dbPath);
        if (file.exists() == false) {
            System.out.println("Error: Invalid ip2region.db file");
        }

        //查询算法
        int algorithm = DbSearcher.BTREE_ALGORITHM; //B-tree
        //DbSearcher.BINARY_ALGORITHM //Binary
        //DbSearcher.MEMORY_ALGORITYM //Memory
        try {
            DbConfig config = new DbConfig();
            DbSearcher searcher = new DbSearcher(config, dbPath);

            //define the method
            Method method = null;
            switch (algorithm) {
                case DbSearcher.BTREE_ALGORITHM:
                    method = searcher.getClass().getMethod("btreeSearch", String.class);
                    break;
                case DbSearcher.BINARY_ALGORITHM:
                    method = searcher.getClass().getMethod("binarySearch", String.class);
                    break;
                case DbSearcher.MEMORY_ALGORITYM:
                    method = searcher.getClass().getMethod("memorySearch", String.class);
                    break;
            }

            DataBlock dataBlock = null;
            if (Util.isIpAddress(ip) == false) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock = (DataBlock) method.invoke(searcher, ip);

            return dataBlock.getRegion();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


}
