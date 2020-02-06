package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object UpdateStateByKeyWordCountScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UpdateStateByKeyWordCountScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(10))
    ssc.checkpoint("hdfs://master:9000/wordcount_checkpoint")

    val lineDstream: ReceiverInputDStream[String] = ssc.socketTextStream("master",9999)

    val wordCountDstream = lineDstream.flatMap(_.split(" ")).map((_,1)).updateStateByKey((values:Seq[Int],state:Option[Int])=>{
      var newValues = state.getOrElse(0)
      for (value <- values) {newValues += value}
      Option(newValues)
    })

    wordCountDstream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
