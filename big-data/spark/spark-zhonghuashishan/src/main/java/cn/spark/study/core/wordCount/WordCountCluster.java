package cn.spark.study.core.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * wordCount提交到集群示例
 */
public class WordCountCluster {

    public static void main(String[] args) {
        // 编写spark应用程序

        //第一步：创建sparkConf对象，设置spark应用的配置信息
        //使用setMaster()可以设置spark应用程序要连接的spark集群的URL，但是如果设置为local 则表示在本地运行
        SparkConf conf = new SparkConf().setAppName("WordCountCluster");

        //第二步：创建JavaSparkContext对象
        //在spark中，SparkContext是spark所有功能的一个入口，你无论使用java、scala，甚至是python编写都必须要有一个
        //sparkContext，他的主要作用，包括初始化spark应用程序所需要的一些核心组件，包括DAGScheduler、TaskScheduler
        //，还会去到spark master节点上进行注册，等等；一句话，sparkContext是spark应用中可以说是最最重要的一个对象
        //在spark中，编写不同类型的spark应用程序，使用的sparkContext是不同的，使用scala，使用的是原生的SparkContext对象
        //如果使用java，那么就是JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //第三步:要针对输入源(hdfs文件,本地文件,等等),创建一个初始的RDD
        //输入源的数据会打散,分配到RDD的每个partition中,从而形成一个初始的分布式的数据集
        //我们这里呢,因为是本地测试,所以就是针对本地文件 SparkContext中,用于根据文件类型的输入源创建RDD的方法叫做textFile()方法
        //在JAVA中,创建的RDD都叫做JavaRDD
        String path = "hdfs://dse:8020/tmp/tianyafu/spark.txt";
        JavaRDD<String> lines = sc.textFile(path);

        //第四步:对初始RDD进行tranformation操作,也就是一些计算操作
        //通常操作会通过创建function,并配合RDD的map,flatMap等算子执行function,通常,如果比较简单,则创建指定function的匿名内部类,但是如果function比价复杂,则会单独创建一个类,作为实现这个function接口的类
        JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //接着,需要以单词作为key,统计每个单词出现的次数,这里要使用reduceByKey这个算子,对每个key对应的value,都进行reduce操作
        JavaPairRDD<String, Integer> wordCountRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCountRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" appeared "+wordCount._2 + " times .");
            }
        });

    }
}
