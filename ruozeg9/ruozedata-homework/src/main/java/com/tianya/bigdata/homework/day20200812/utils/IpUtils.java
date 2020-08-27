package com.tianya.bigdata.homework.day20200812.utils;
import org.apache.hadoop.conf.Configuration;
import org.lionsoul.ip2region.*;

import java.io.IOException;
import java.lang.reflect.Method;

public class IpUtils {

    private static DbConfig config;

    private static String dbPath = "/home/hadoop/app/xk-log-etl/data/ip2region.db";

    private static DbSearcher searcher;

    /**
     * 建立数据库文件连接
     * @throws Exception
     */
    public static void getConnect() throws Exception{
        config = new DbConfig();
        searcher = new DbSearcher(config, dbPath);
    }
    /**
     * 查询ip
     * @param ip
     * @return
     * @throws Exception
     */
    public static String  analysisIp(String ip) throws Exception{
        if (searcher == null){
            getConnect();
        }
        Method method = searcher.getClass().getMethod("btreeSearch", String.class);
        DataBlock dataBlock = null;
        if (Util.isIpAddress(ip) == false ) {
            System.out.println("Error: Invalid ip address");
        }
        dataBlock  = (DataBlock) method.invoke(searcher, ip);
        return dataBlock.getRegion();
    }
    
        /**
     * 关闭连接
     * @throws IOException
     */
    public static void closeConnect() throws IOException {
        searcher.close();
    }
    
}
