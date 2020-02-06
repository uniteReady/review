package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

public class WindowHotWord {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("WindowHotWord").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //这里的搜索日志格式为 username searchWord
        JavaReceiverInputDStream<String> searchLogsDstream = jssc.socketTextStream("master", 9999);
        JavaPairDStream<String, Integer> pairDStream = searchLogsDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1], 1);
            }
        });
        JavaPairDStream<String, Integer> searchWordCountDstream = pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));

        JavaPairDStream<String, Integer> resultDstream = searchWordCountDstream.transformToPair(new Function2<JavaPairRDD<String, Integer>, Time, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> tuple, Time v2) throws Exception {
                JavaPairRDD<Integer, String> searchCountWordRDD = tuple.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
                        return new Tuple2<>(v1._2, v1._1);
                    }
                });
                JavaPairRDD<Integer, String> sortedRDD = searchCountWordRDD.sortByKey(false);

                JavaPairRDD<String, Integer> sortedWordCountRDD = sortedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> v3) throws Exception {
                        return new Tuple2<>(v3._2, v3._1);
                    }
                });
                List<Tuple2<String, Integer>> top3s = sortedWordCountRDD.take(3);
                for (Tuple2<String, Integer> top3 : top3s) {
                    System.out.println(top3._1+": "+ top3._2);
                }
                return tuple;

            }
        });

        resultDstream.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
