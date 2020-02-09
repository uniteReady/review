package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于updateStateByKey算子的缓存机制的实时wordCount示例,最后将结果持久化到mysql
 * CREATE TABLE `wordcount` (
 *   `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
 *   `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 *   `word` varchar(255) DEFAULT NULL,
 *   `count` int(11) DEFAULT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='大数据wordcount';
 */
public class UpdateStateByKeyWordCountPersist {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWordCountPersist").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        //第一点：如果要使用updateStateByKey，就必须设置一个checkpoint目录，开启checkpoint机制
        //这样的话才能把每个key对应的state除了在内存中有之外，在checkpoint目录中也有，因为要长期保存一份key的state
        //那么spark streaming是要求必须用checkpoint的，以便于在内存数据丢失的时候，可以从checkpoint中恢复数据
        jssc.checkpoint("hdfs://master:9000/wordcount_checkpoint");

        //先实现基本的wordcount逻辑
        JavaReceiverInputDStream<String> linesDstream = jssc.socketTextStream("master", 9999);

        JavaDStream<String> wordsDstream = linesDstream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairDStream = wordsDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCountDstream = pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            /**
             *这里两个参数：
             * 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数第一个参数，values，相当于是这个batch中，这个
             * key的新的值，可能有多个吧，比如一个hello，可能有2个1，(hello,1),(hello,1)，那么传入的values就是(1,1)
             * 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
             * @param values
             * @param state
             * @return
             * @throws Exception
             */
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
                if (state.isPresent()) {
                    newValue = state.get();
                }
                for (Integer value : values) {
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        });

        //每次得到当前所有单词的统计次数后，将其写入mysql存储，进行持久化，以便于后续的j2ee进行查询
        wordCountDstream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> wordCountRDD) throws Exception {
                //调用RDD的foreachPartition方法
                wordCountRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        //给每个partition获取一个连接
                        Connection conn = ConnectionPool.getConnection();
                        Tuple2<String,Integer> wordcount = null;
                        while (wordCounts.hasNext()){
                           wordcount =  wordCounts.next();
                           String sql = "insert into wordcount(word,count) values(?,?)";
                            PreparedStatement stat = conn.prepareStatement(sql);
                            stat.setString(1,wordcount._1);
                            stat.setInt(2,wordcount._2);
                            stat.executeUpdate();
                        }
                        //用完连接还回去
                        ConnectionPool.returnConnection(conn);
                    }
                });
            }
        });

        jssc.start();
        jssc.awaitTermination();

        jssc.stop();

    }
}
