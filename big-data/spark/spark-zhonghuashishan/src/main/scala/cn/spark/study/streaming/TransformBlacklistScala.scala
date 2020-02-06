package cn.spark.study.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

object TransformBlacklistScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TransformBlacklistScala").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(10))

    val sc: SparkContext = ssc.sparkContext
    val array = Array(("tom", true))
    val blacklist: RDD[(String, Boolean)] = sc.parallelize(array)
    val adsClickLogDstream: ReceiverInputDStream[String] = ssc.socketTextStream("master",9999)

    val usernameDateUsernameDstream: DStream[(String, String)] = adsClickLogDstream.map(x =>(x.split(" ")(1),x))

    val result = usernameDateUsernameDstream.transform(
      x =>{
        val joinRDD: RDD[(String, (String, Option[Boolean]))] = x.leftOuterJoin(blacklist)
        val validRDD = joinRDD.filter(tuple =>{
          if(tuple._2._2.getOrElse(false)){
            false
          }else{
            true
          }
        })
        val validAds: RDD[String] = validRDD.map(_._2._1)
        validAds
      }
    )

    result.print()




    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
