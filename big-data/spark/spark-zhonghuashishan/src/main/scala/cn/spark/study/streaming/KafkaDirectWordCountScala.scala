package cn.spark.study.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

object KafkaDirectWordCountScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaDirectWordCountScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slave01:9092,slave02:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_streaming_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("sparkstreaming_kafka")

    val lineDstream:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics,kafkaParams)
    )

    val wordsDstream: DStream[String] = lineDstream.flatMap(_.value().split(" "))

    val pairDstream: DStream[(String, Int)] = wordsDstream.map((_,1))

    val wordCountDstream: DStream[(String, Int)] = pairDstream.reduceByKey(_+_ )

    wordCountDstream.print()





    ssc.start()
    ssc.awaitTermination()


  }

}
