package cn.spark.study.core.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * wordCount 实现降序排序
 */
public class SortWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sortWordCount");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\hello.txt";
        JavaRDD<String> linesRDD = sc.textFile(path);

        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> javaPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceByKeyRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> countWordRDD = reduceByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        JavaPairRDD<Integer, String> sortedRDD = countWordRDD.sortByKey(false);

        JavaRDD<Tuple2<String, Integer>> wordCountSortedRDD = sortedRDD.map(new Function<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> v1) throws Exception {
                return new Tuple2<>(v1._2, v1._1);
            }
        });

        wordCountSortedRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("word: " + t._1 + ", count: " + t._2);
            }
        });


        sc.close();
    }
}
