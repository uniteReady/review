package cn.spark.study.core;

import com.sun.javafx.scene.control.skin.LabeledImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class Persist {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("persist");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "";

        /**
         * cache()或者persist()的使用 是有规则的
         * 必须再tranformation或者textFile等创建一个RDD之后，直接连续调用cache()或者persist()才可以
         * 如果先创建一个RDD，然后单独另一行执行cache()或者persist() 是没有用的 而且会报错
         */
        JavaRDD<String> linesRDD = sc.textFile(path).persist(StorageLevel.MEMORY_AND_DISK());
//        JavaRDD<String> linesRDD = sc.textFile(path).cache();
        long startTime = System.currentTimeMillis();
        System.out.println(linesRDD.count());
        long endTime = System.currentTimeMillis();
        System.out.println("cost time: " + (endTime - startTime));

        startTime = System.currentTimeMillis();
        System.out.println(linesRDD.count());
        endTime = System.currentTimeMillis();
        System.out.println("cost time: " + (endTime - startTime));


        sc.close();


    }
}
