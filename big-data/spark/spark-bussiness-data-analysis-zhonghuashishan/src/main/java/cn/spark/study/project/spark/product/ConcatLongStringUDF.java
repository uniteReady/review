package cn.spark.study.project.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * 自定义UDF函数  连接一个Long值和一个String类型 可以指定分隔符
 */
public class ConcatLongStringUDF implements UDF3<Long,String,String,String> {
    @Override
    public String call(Long v1, String s, String split) throws Exception {
        return String.valueOf(v1)+ split +s;
    }
}
