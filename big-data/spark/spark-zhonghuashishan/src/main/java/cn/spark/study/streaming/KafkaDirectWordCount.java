package cn.spark.study.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * http://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html#storing-offsets
 * 基于Kafka Direct方式的word count 程序
 * <p>
 * After a context is defined, you have to do the following.
 * <p>
 * Define the input sources by creating input DStreams.
 * Define the streaming computations by applying transformation and output operations to DStreams.
 * Start receiving data and processing it using streamingContext.start().
 * Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination().
 * The processing can be manually stopped using streamingContext.stop().
 */
public class KafkaDirectWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));



        //创建一份kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "master:9092,slave01:9092,slave02:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "kafka_streaming_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics =Arrays.asList("sparkstreaming_kafka");

        JavaInputDStream<ConsumerRecord<String, String>> lineDStream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));


        JavaDStream<String> wordsDstream = lineDStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                return Arrays.asList(consumerRecord.value().split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairDStream = wordsDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCountDstream = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCountDstream.print();


        jssc.start();
        jssc.awaitTermination();



    }
}
