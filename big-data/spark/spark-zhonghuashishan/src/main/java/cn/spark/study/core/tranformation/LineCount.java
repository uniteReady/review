package cn.spark.study.core.tranformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class LineCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCount");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path  = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\hello.txt";

        JavaRDD<String> lines = sc.textFile(path);

        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> resultRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> lineCount) throws Exception {
                System.out.println(lineCount._1 + " appeared "+ lineCount._2 + " times.");
            }
        });


        sc.close();
    }
}
