package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 * 1.实现自定义的key，要实现Ordered接口和Serialized接口，在key中实现自己对多个列的排序算法
 * 2.将包含文本的RDD，映射成key为自定义key， value为文本的JavaPairRDD
 * 3.进行排序
 * 4.将自定义key去掉，保留文本
 */
public class SecondarySort {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("secondarySort");

        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\sort.txt";
        JavaRDD<String> lineRDD = sc.textFile(path);
        JavaPairRDD<SecondarySortKey,String> pairs = lineRDD.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] splits = s.split(" ");
                SecondarySortKey key = new SecondarySortKey();
                key.setFirst(Integer.parseInt(splits[0]));
                key.setSecond(Integer.parseInt(splits[1]));
                return new Tuple2<>(key,s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortedRDD = pairs.sortByKey();

        sortedRDD.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
