package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于updateStateByKey算子的缓存机制的实时wordCount示例
 */
public class UpdateStateByKeyWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local[2]");
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

        wordCountDstream.print();


        jssc.start();
        jssc.awaitTermination();

        jssc.stop();

    }
}
