package cn.spark.study.core.actionOperation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * action算子操作实战
 */
public class ActionOperation {

    public static void main(String[] args) {
//        reduce();
//        collect();
//        count();
//        take();
        //saveAsTextFile();
        countByKey();
    }

    private static void countByKey() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("countByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, String>> students = Arrays.asList(
                new Tuple2<String, String>("class1", "leo"),
                new Tuple2<String, String>("class2", "jack"),
                new Tuple2<String, String>("class2", "tom"),
                new Tuple2<String, String>("class2", "jerry"),
                new Tuple2<String, String>("class1", "marry")
        );
        JavaPairRDD<String,String> javaPairRDD = sc.parallelizePairs(students);

        Map<String, Long> stringLongMap = javaPairRDD.countByKey();
        for (Map.Entry<String, Long> entry : stringLongMap.entrySet()) {
            System.out.println("class: "+entry.getKey()+" , studentCount: "+ entry.getValue());
        }

        sc.close();
    }

    public static void  saveAsTextFile(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("take");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> numsRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> mapRDD = numsRDD.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\saveAsTextFileJava.txt";
        mapRDD.saveAsTextFile(path);

        sc.close();
    }

    public  static  void  take(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("take");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> numsRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        List<Integer> result = numsRDD.take(5);

        for (Integer integer : result) {
            System.out.println(integer);
        }

        sc.close();
    }


    public static  void  count(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> numsRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        long count = numsRDD.count();
        System.out.println(count);

        sc.close();
    }

    public static void  collect(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("collect");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> numsRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> doubleNum = numsRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        List<Integer> resultList = doubleNum.collect();
        for (Integer integer : resultList) {
            System.out.println(integer);
        }

        sc.close();
    }

    public static  void  reduce(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduce");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);
        Integer result = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(result);
        sc.close();
    }
}
