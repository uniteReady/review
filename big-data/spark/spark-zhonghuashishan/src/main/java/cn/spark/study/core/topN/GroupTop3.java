package cn.spark.study.core.topN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 分组取topN
 * 每个班级排名前3的成绩
 */
public class GroupTop3 {

    public static void main(String[] args) {
        int topN = 4;
        SparkConf conf = new SparkConf().setMaster("local").setAppName("groupTopN");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\score.txt";

        JavaRDD<String> lineRDD = sc.textFile(path);

        JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lineSplited = s.split(" ");
                return new Tuple2<>(lineSplited[0], Integer.parseInt(lineSplited[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupedRDD = pairRDD.groupByKey();

        JavaPairRDD<String, List<Integer>> groupTop3RDD = groupedRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, List<Integer>>() {
            @Override
            public Tuple2<String, List<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {

                String className = t._1;
                Integer[] top3Array = new Integer[topN];
                Iterable<Integer> scores = t._2;

                Iterator<Integer> it = scores.iterator();

                while (it.hasNext()) {
                    Integer score = it.next();

                    for (int i = 0; i < top3Array.length; i++) {
                        if (top3Array[i] == null) {
                            top3Array[i] = score;
                            break;
                        } else if (score > top3Array[i]) {
                            for (int j = topN - 1; j > i; j--) {
                                top3Array[j] = top3Array[j - 1];
                            }
                            top3Array[i] = score;
                            break;
                        }
                    }

                }
                return new Tuple2<>(className, Arrays.asList(top3Array));
            }
        });

        groupTop3RDD.foreach(new VoidFunction<Tuple2<String, List<Integer>>>() {
            @Override
            public void call(Tuple2<String, List<Integer>> t) throws Exception {
                System.out.println("className: " + t._1);
                for (Integer integer : t._2) {
                    System.out.println(integer);
                }
                System.out.println("=====================");
            }
        });


        sc.close();
    }
}
