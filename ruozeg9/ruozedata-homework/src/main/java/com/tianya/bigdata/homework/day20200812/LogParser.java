package com.tianya.bigdata.homework.day20200812;

import com.tianya.bigdata.homework.day20200812.domain.Access;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.lionsoul.ip2region.*;

import java.beans.Transient;
import java.io.*;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class LogParser implements Serializable {

    public static final SimpleDateFormat FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);

    private static DbConfig config;

    //    private static String dbPath = "ip2region.db";
//    private static String dbPath = "/ruozedata/dw/data/ip2region.db";
    private static String dbPath = "F:\\study\\workspace\\review\\ruozeg9\\ruozedata-homework\\src\\main\\resources\\ip2region.db";




    transient public static DbSearcher searcher;

    static {
        try {
            searcher = getConnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 建立数据库文件连接
     *
     * @throws Exception
     */

    public static DbSearcher getConnect() throws Exception {
        config = new DbConfig();
//        searcher = new DbSearcher(config, dbPath);
        searcher = new DbSearcher(config, dbPath);
        return searcher;
    }


    public static DbSearcher getConnect(FileSystem fileSystem) throws Exception {
        FSDataInputStream in = fileSystem.open(new Path(dbPath));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, byteArrayOutputStream, 2048);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        config = new DbConfig();
//        searcher = new DbSearcher(config, dbPath);
        searcher = new DbSearcher(config, bytes);
        return searcher;
    }

    /**
     * 解析日志的时间
     *
     * @param time
     * @return
     * @throws Exception
     */
    public static Access analysisTime(String time, Access access) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:hh:mm:ss");

        Date timeParse = null;
        try {
            timeParse = simpleDateFormat.parse(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timeParse);
            String year = String.valueOf(calendar.get(Calendar.YEAR));
            String month = String.valueOf(calendar.get(Calendar.MONTH));
            String day = String.valueOf(calendar.get(Calendar.DAY_OF_MONTH));
            month = Integer.parseInt(month) < 10 ? "0" + month : month;
            day = Integer.parseInt(day) < 10 ? "0" + day : day;
            access.setYear(year);
            access.setMonth(month);
            access.setDay(day);
        } catch (ParseException e) {
            System.out.println(time);
            e.printStackTrace();
        }
        return access;

    }

    public static void main(String[] args) throws Exception {
        String log = "[01/01/2019:06:40:38]\t182.82.87.180\t-\t2837\t-\tPOST\thttp://tianyafu651/video/av5216721651?a=b&c=d\t404\t3886\t2330\tHIT\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";
//        Access access = parseLog3(log);
//        System.out.println(access);
        /*BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        int count = 0;
        while ((line = reader.readLine())!=null){
            try{
                Access access = parseLog3(line);
//                System.out.println(access);
                if("20193".equals(access.getYear())){
                    System.out.println(line);
                    break;
                }
                *//*count ++;
                if(count > 3){
                    break;
                }*//*

            }catch (Exception e){

            }
        }*/

    }


    /**
     * 分析URL
     *
     * @param url
     * @param access
     * @return
     * @throws Exception
     */
    public static Access analysisUrl(String url, Access access) {
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

        return access;
    }


    /**
     * 查询ip
     *
     * @param ip
     * @return
     * @throws Exception
     */
    public static Access analysisIp(String ip, Access access, DbSearcher searcher) {
        try {
            Method method = searcher.getClass().getMethod("btreeSearch", String.class);
            DataBlock dataBlock = null;
            if (Util.isIpAddress(ip) == false) {
                System.out.println("Error: Invalid ip address");
            }
            dataBlock = (DataBlock) method.invoke(searcher, ip);

            String ipInfo = dataBlock.getRegion();
            String[] ipInfos = ipInfo.split("\\|");
            String province = ipInfos[2];
            String city = ipInfos[3];
            String isp = ipInfos[4];
            access.setProvince(province);
            access.setCity(city);
            access.setIsp(isp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return access;
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnect() throws IOException {
        searcher.close();
    }


    public static Access parseLog3(String log, DbSearcher searcher) throws NumberFormatException {
        Access access = new Access();
        String[] splits = log.split("\t");
        //解析时间
        String time = splits[0];
        time = time.substring(1, time.length() - 1);
        access = analysisTime(time, access);

        //解析ip
        String ip = splits[1];
        access = analysisIp(ip, access, searcher);

        String proxyIp = splits[2];
        String responseTime = splits[3];
        String referer = splits[4];
        String method = splits[5];
        //解析URL
        String url = splits[6];
        access = analysisUrl(url, access);

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

        access.setResponseSize(Long.parseLong(responseSize));
        return access;
    }


    public static Access parseLog(DbSearcher searcher, String log) throws ParseException {
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


    public static Access parseLog2(DbSearcher searcher, String log) {
//        String log = "[29/Jan/2019:01:51:04 +0800]\t61.236.169.40\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";
//        String log = "[29/Jan/2019:01:51:04 +0800]\t61.235.87.102\t-\t3415\t-\tPOST\thttp://tianyafu0/video/av52167210?a=b&c=d\t404\t1472\t1009\tMISS\tMozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）\ttext/html";

        try {
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
        } catch (Exception e) {
            return null;
        }


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
