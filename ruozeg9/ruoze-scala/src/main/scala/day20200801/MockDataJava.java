package day20200801;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;
import java.util.StringJoiner;

import static org.datanucleus.NucleusContext.random;

/**
 * [9/Jun/2015:01:58:09 +0800] 192.168.15.75 - 1542 "-" "GET http://www.aliyun.com/index.html" 200 191 2830 MISS "Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)" "text/html"
 */
public class MockDataJava {

    public static final SimpleDateFormat FORMAT1 = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);

    public static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("out/access.log"))));

        for (int i = 0; i < 450000; i++) {
            StringJoiner sj = new StringJoiner(" ");
            String dateStr = mockDateStr(); //随机生成时间
            String ipStr = MockIpStr(); //随机生成IP
            String agentIp = "-"; //随机生成代理IP
            Integer responseTime = mockIntegerData(); //随机生成响应时间
            String referer = "\"-\""; //随机生成referer
            String method = mockMethod();
            String url = mockUrl();
            Integer httpCode = mockHttpCode();
            Integer requestSize = mockIntegerData();
            String responseSize = "";
            if(i % 17 ==0){
                responseSize = "-";
            }else{
                responseSize = mockIntegerData()+"";
            }
            String cacheStatus = "MISS";
            String UAHead = "Mozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）";
            String type = "text/html";
            sj.add(dateStr)
                    .add(ipStr)
                    .add(agentIp)
                    .add(responseTime+"")
                    .add(referer)
                    .add(method)
                    .add(url)
                    .add(httpCode+"")
                    .add(requestSize+"")
                    .add(responseSize)
                    .add(cacheStatus).add(UAHead).add(type);
//            System.out.println(sj.toString());
            writer.write(sj.toString());
            writer.write("\n");

        }

        writer.flush();
        writer.close();

    }


    /**
     * 随机生成URL
     * @return
     */
    public static String mockUrl(){
        String preUrl = "http://www.aliyun.com/";
        String sufUrl = "/index.html";
        return preUrl +RANDOM .nextInt(10)+sufUrl;
    }

    /**
     * 随机生成httpcode
     * @return
     */
    public static Integer mockHttpCode(){
        Integer[] httpcodes = new Integer[]{200,500,404};
        return httpcodes[RANDOM.nextInt(httpcodes.length)];

    }


    /**
     * 随机生成请求方式
     * @return
     */
    public static String mockMethod(){
        String[] methods = new String[]{"GET","POST"};
        Random random = new Random();
        random.nextInt(methods.length);
        return methods[random.nextInt(methods.length)];
    }


    /**
     * 随机生成5000内的整数
     * @return
     */
    public static Integer mockIntegerData(){
        Random random = new Random();
        return random.nextInt(5000);
    }

    /*
     * 随机生成国内IP地址
     */
    public static String MockIpStr() {

        // ip范围
        int[][] range = { { 607649792, 608174079 },// 36.56.0.0-36.63.255.255
                { 1038614528, 1039007743 },// 61.232.0.0-61.237.255.255
                { 1783627776, 1784676351 },// 106.80.0.0-106.95.255.255
                { 2035023872, 2035154943 },// 121.76.0.0-121.77.255.255
                { 2078801920, 2079064063 },// 123.232.0.0-123.235.255.255
                { -1950089216, -1948778497 },// 139.196.0.0-139.215.255.255
                { -1425539072, -1425014785 },// 171.8.0.0-171.15.255.255
                { -1236271104, -1235419137 },// 182.80.0.0-182.92.255.255
                { -770113536, -768606209 },// 210.25.0.0-210.47.255.255
                { -569376768, -564133889 }, // 222.16.0.0-222.95.255.255
        };

        Random rdint = new Random();
        int index = rdint.nextInt(10);
        String ip = num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    /*
     * 将十进制转换成ip地址
     */
    public static String num2ip(int ip) {
        int[] b = new int[4];
        String x = "";

        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

        return x;
    }




    public static String mockDateStr(){
        Date date = randomDate("2019-01-01","2019-01-31");
        return "["+FORMAT1.format(date)+"]";
    }


    private static Date randomDate(String beginDate, String endDate){
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);
            Date end = format.parse(endDate);

            if(start.getTime() >= end.getTime()){
                return null;
            }
            long date = random(start.getTime(),end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static long random(long begin,long end){
        long rtn = begin + (long)(Math.random() * (end - begin));
        if(rtn == begin || rtn == end){
            return random(begin,end);
        }
        return rtn;
    }
}
