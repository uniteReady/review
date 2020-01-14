package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;

public class BroadcastDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("broadcast");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int fact = 3;

        Broadcast<Integer> factBroadcast = sc.broadcast(fact);

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        JavaRDD<Integer> mapRDD = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                Integer factValue = factBroadcast.value();
                return v1 * factValue;
            }
        });

        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        sc.close();
    }
}
