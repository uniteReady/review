package com.tianya.bigdata.homework.day20200901

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LogGroupByKeyApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(List(
      "100000,一起看|电视剧|军旅|士兵突击,1,1",
      "100000,一起看|电视剧|军旅|士兵突击,1,0",
      "100001,一起看|电视剧|军旅|我的团长我的团,1,1")
    )

    val kvRDD: RDD[((String, String), (Int, Int))] = dataRDD.flatMap(data => {
      val splits = data.split(",")
      val id = splits(0)
      val nav = splits(1)
      val imp = splits(2).toInt
      val click = splits(3).toInt
      val navs = nav.split("\\|")
      navs.map(x => ((id, x), (imp, click)))
    }).cache

    //reduceByKey
    kvRDD.reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).foreach(println)
    //groupByKey
    kvRDD.groupByKey().mapValues(x => {
      var showSum = 0
      var clickSum = 0
      val iter = x.iterator
      while (iter.hasNext) {
        val tuple = iter.next()
        showSum = showSum + tuple._1
        clickSum = clickSum + tuple._2

      }
      (showSum, clickSum)
    }).foreach(println)


    sc.stop()
  }

}
