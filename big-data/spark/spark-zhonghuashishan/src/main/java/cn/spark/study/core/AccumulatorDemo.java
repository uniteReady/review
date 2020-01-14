package cn.spark.study.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class AccumulatorDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Accumulator");

        JavaSparkContext sc = new JavaSparkContext(conf);

        LongAccumulator sum = sc.sc().longAccumulator("sumAccumulator");

        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        JavaRDD<Integer> mapRDD = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                sum.add(v1);
                return v1 * 2;
            }
        });

        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        System.out.println("========================");
        System.out.println(sum.value());

        sc.close();
    }
}
