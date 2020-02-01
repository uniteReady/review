package cn.spark.study.core.tranformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TranformationOperation {

    public static void main(String[] args) {
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 60),
                new Tuple2<String, Integer>("class2", 95),
                new Tuple2<String, Integer>("class1", 80)
        );


//        map();
//        filter();
//        flatMap();
//        groupByKey(scores);
//        reduceByKey(scores);
//        sortByKey(scores);
//        join();
        cogroup();
    }

    /**
     * cogroup案例:打印学生成绩
     * <p>
     * cogroup 与join的不同
     * 相当于是一个key join上的所有value都放到一个Iterable里面去了
     */
    public static void cogroup() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("cogroup");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(3, 80),
                new Tuple2<Integer, Integer>(2, 50)
        );
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom")
        );
        //并行化两个RDD
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = studentRDD.cogroup(scoreRDD);

        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("student id : " + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student score: " + t._2._2);
            }
        });

        sc.close();


    }


    /**
     * join案例:打印学生成绩
     */
    public static void join() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60)
        );
        //并行化两个RDD
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        //使用join算子关联两个RDD
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = studentRDD.join(scoreRDD);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("student id: " + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student score: " + t._2._2);
                System.out.println("=====================================");
            }
        });
        sc.close();
    }


    /**
     * sortByKey案例:将学生分数进行排序 按降序排序
     */
    public static void sortByKey(List<Tuple2<String, Integer>> scores) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sortByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> scoresList = new ArrayList<Tuple2<Integer, String>>();
        for (Tuple2<String, Integer> score : scores) {
            scoresList.add(new Tuple2<Integer, String>(score._2, score._1));
        }
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(scoresList);
        JavaPairRDD<Integer, String> sortedScoreRDD = pairRDD.sortByKey(false);
        sortedScoreRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + " : " + t._2);
            }
        });
        sc.close();
    }

    /**
     * reduceByKey案例:统计每个班级的总分
     */
    public static void reduceByKey(List<Tuple2<String, Integer>> scores) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("reduceByKey");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);

        JavaPairRDD<String, Integer> resultRDD = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("class: " + t._1 + " , scoreSum: " + t._2);
            }
        });


        sc.close();

    }


    /**
     * groupByKey算子案例:将每个班级的成绩进行分组
     */
    public static void groupByKey(List<Tuple2<String, Integer>> scores) {
        SparkConf conf = new SparkConf().setAppName("TranformationOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);

        JavaPairRDD<String, Iterable<Integer>> groupedScore = scoresRDD.groupByKey();

        groupedScore.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> it = t._2.iterator();

                while (it.hasNext()) {
                    System.out.println(it.next());
                }
                System.out.println("===================");
            }
        });


        sc.close();
    }


    /**
     * flatMap算子案例:将文本行拆成多个单词
     */
    public static void flatMap() {

        SparkConf conf = new SparkConf().setAppName("TranformationOperation").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String text = "hello world hello spark hello hadoop hello elasticsearch";

        JavaRDD<String> textRDD = sc.parallelize(Arrays.asList(text));

        JavaRDD<String> flatMapRDD = textRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        flatMapRDD.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        sc.close();

    }

    /**
     * filter算子案例:过滤掉集合中的奇数
     */
    public static void filter() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TranformationOperation");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        JavaRDD<Integer> filterRDD = numsRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        filterRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();

    }


    /**
     * map算子案例:将集合中每一个元素乘以2
     */
    public static void map() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TranformationOperation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);
        JavaRDD<Integer> multipleNumberRDD = numsRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        multipleNumberRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
