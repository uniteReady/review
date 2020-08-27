package com.tianya.bigdata.homework.day20200812.utils;

import java.lang.reflect.Array;
import java.util.Arrays;

public class HttpUtils {

    public static String[] analysisHttp(String url){
        // http://xiekai.com/7/index.html?a=b&c=d
        String[] httpSplit = url.split("//");
        String http = httpSplit[0];
        http = http.substring(0,http.length()-1);
        String[] domainSplit = httpSplit[1].split("/");
        String domain = domainSplit[0];
        String pathAngParam = httpSplit[1].substring(domain.length());
        String[] pathAngParamSpilt = pathAngParam.split("\\?");
        String path = pathAngParamSpilt[0].substring(1);
        String[] result = new String[]{http,domain,path};

        return result;
    }

}
