package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于tranform的实时黑名单过滤
 */
public class TransformBlacklist {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("TransformBlacklist").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        //先做一份黑名单数据
        List<Tuple2<String,Boolean>> blacklist = new ArrayList<>();
        blacklist.add(new Tuple2<>("tom",true));
        JavaSparkContext sc = jssc.sparkContext();
        JavaPairRDD<String, Boolean> blacklistRDD = sc.<String, Boolean>parallelizePairs(blacklist);

        //用户对我们的网站上的广告可以进行点击，点击之后，是不是要进行实时计费，点一下，算一次钱
        // 但是对于那些帮助无良商家刷广告的人，那么我们有一个黑名单，只要是黑名单中的用户点击的广告，我们就给过滤掉
        //这里的日志格式 要简化一下  就是date username的方式
        JavaReceiverInputDStream<String> adsClickLog = jssc.socketTextStream("master", 9999);


        //所以，要先对输入的数据，进行一下转换操作，变成(username,date username)的格式，
        // 以便于，后面对每个batch RDD，与定义好的黑名单RDD进行join操作
        JavaPairDStream<String, String> pairDStream = adsClickLog.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                return new Tuple2<>(line.split(" ")[1], line);
            }
        });

        //然后就可以进行transform操作了，将每个batch的RDD，与黑名单RDD进行join、filter、map等操作，进行实时黑名单过滤

        JavaDStream<String> validDstream = pairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                //这里用了leftOuterJoin，哪怕一个user没有被join到 也是会被保存下来的
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> leftOuterJoinRDD = userAdsClickLogRDD.<Boolean>leftOuterJoin(blacklistRDD);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> validPairRDD = leftOuterJoinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        //这里的tuple，就是每个用户，对应的访问日志，和在和名单中的状态
                        if (tuple._2._2().isPresent() && tuple._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });
                JavaRDD<String> validStringRDD = validPairRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        return v1._2._1;
                    }
                });
                return validStringRDD;
            }
        });

        //其实在真实企业场景中，这里后面就可以走写入kafka等消息队列，然后开发一个专门的后台服务，
        // 作为广告计费服务，执行实时的广告计费，这里就是只拿到了有效的广告点击
        validDstream.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.stop();

    }
}
