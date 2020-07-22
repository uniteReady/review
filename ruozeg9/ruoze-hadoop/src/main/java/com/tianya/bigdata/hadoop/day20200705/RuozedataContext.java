package com.tianya.bigdata.hadoop.day20200705;

import java.util.HashMap;

/**
 * 上下文
 */
public class RuozedataContext {

    private HashMap<Object, Object> cacheMap = new HashMap<>();


    /**
     * 获取上下文
     * @return
     */
    public HashMap<Object, Object> getCacheMap(){
        return cacheMap;
    }

    /**
     * 获取上下文的key对应的value
     * @param key
     * @return
     */
    public Object get(Object key){
        return cacheMap.get(key);
    }

    /**
     * 写数据到上下文中
     * @param key
     * @param value
     */
    public void write(Object key, Object value){
        cacheMap.put(key,value);
    }
}
