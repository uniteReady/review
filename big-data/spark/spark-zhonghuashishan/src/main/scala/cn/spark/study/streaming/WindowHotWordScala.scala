package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object WindowHotWordScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WindowHotWordScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    val searchLogsDstream: ReceiverInputDStream[String] = ssc.socketTextStream("master",9999)

    val pairDstream: DStream[(String, Int)] = searchLogsDstream.map(x =>(x.split(" ")(1),1))

    val searchWordCountsDstream: DStream[(String, Int)] = pairDstream.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Durations.seconds(60),Durations.seconds(10))

    searchWordCountsDstream.transform(
      searchWordCountRDD =>{
      val countWordRDD: RDD[(Int, String)] = searchWordCountRDD.map(_.swap)
        val sortedRDD: RDD[(Int, String)] = countWordRDD.sortByKey(false)
        val sortedWordCountRDD: RDD[(String, Int)] = sortedRDD.map(_.swap)
        val results: Array[(String, Int)] = sortedWordCountRDD.take(3)
        for (elem <- results) {
          println(elem._1 + ": "+ elem._2)
        }
        sortedWordCountRDD
    }).print()




    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
