package cn.spark.study.core.topN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Top3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("top3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\top.txt";
        JavaRDD<String> lineRDD = sc.textFile(path);

        JavaRDD<Integer> numsRDD = lineRDD.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return Integer.valueOf(v1);
            }
        });
        JavaPairRDD<Integer, Integer> pairRDD = numsRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, 1);
            }
        });

        JavaPairRDD<Integer, Integer> sortedRDD = pairRDD.sortByKey(false);

        List<Integer> top3 = sortedRDD.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._1;
            }
        }).take(3);

        for (Integer integer : top3) {
            System.out.println(integer);
        }
        sc.close();
    }
}
