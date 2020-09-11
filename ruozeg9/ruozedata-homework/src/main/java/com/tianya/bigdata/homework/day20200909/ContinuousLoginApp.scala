package com.tianya.bigdata.homework.day20200909

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD


object ContinuousLoginApp extends Logging {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val continuousLoginNum: Int = conf.get("spark.login.continuous.num", "3").toInt
    val filePath = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/homework/day20200909/data/data.txt"
    val linesRDD: RDD[String] = sc.textFile(filePath)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()

    val pairRDD: RDD[(String, String)] = linesRDD.distinct.map(line => {
      val splits: Array[String] = line.split(",")
      (splits(0), splits(1))
    })


    //把同一个用户的所有登录时间放入一个List中
    val seqOp: (List[String], String) => List[String] = (x, y) => x :+ y
    val combOp: (List[String], List[String]) => List[String] = (x, y) => x ++ y
    val aggrRDD: RDD[(String, List[String])] = pairRDD.aggregateByKey(List[String]())(seqOp, combOp)
    //同一用户的记录排序后，添加索引
    val zipWithIndexRDD: RDD[(String, List[(String, Int)])] = aggrRDD.mapValues(_.sorted.zipWithIndex)
    //每天的登录时间减去索引
    val diffIndexRDD: RDD[(String, String)] = zipWithIndexRDD.flatMapValues(x => {
      x.map(tuple => {
        val dateStr: String = tuple._1
        val index: Int = tuple._2
        calendar.setTime(sdf.parse(dateStr))
        calendar.add(Calendar.DAY_OF_YEAR, -index)
        sdf.format(calendar.getTime)
      })
    })

    //wc 算出连续continuousLoginNum天登录的记录
    val value: RDD[(String, Int)] = diffIndexRDD
      .map(x => (x._1 + "\t" + x._2, 1))
      .reduceByKey(_ + _)
      .filter(_._2 == continuousLoginNum)
    //根据开始时间和连续天数计算出结束时间
    val resultRDD: RDD[String] = value.map(x => {
      val key: String = x._1
      val continuousDays: Int = x._2
      val splits: Array[String] = key.split("\t")
      val user = splits(0)
      val startTime = splits(1)
      calendar.setTime(sdf.parse(startTime))
      calendar.add(Calendar.DAY_OF_YEAR, continuousDays)
      val endTime = sdf.format(calendar.getTime)
      s"$user,$continuousDays,$startTime,$endTime"
    })

    resultRDD.foreach(println)


    sc.stop()
  }

}
