package com.tianya.bigdata.homework.day20200812;

import java.io.File;
import java.lang.reflect.Method;

import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;
import org.lionsoul.ip2region.Util;

public class IPUtils {

    public static String getCityInfo(DbSearcher searcher,String ip){
        //查询算法
        int algorithm = DbSearcher.BTREE_ALGORITHM; //B-tree
//DbSearcher.BINARY_ALGORITHM //Binary
//DbSearcher.MEMORY_ALGORITYM //Memory
        try {
                /*DbConfig config = new DbConfig();
                DbSearcher searcher = new DbSearcher(config, dbPath);*/

//define the method
            Method method = null;
            switch ( algorithm )
            {
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
            if ( Util.isIpAddress(ip) == false ) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock = (DataBlock) method.invoke(searcher, ip);

            return dataBlock.getRegion();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "城市Id|国家|区域|省份|城市|ISP";
    }


    public static void main(String[] args) throws Exception{
//        System.err.println(getCityInfo("220.248.12.158"));
    }
}
