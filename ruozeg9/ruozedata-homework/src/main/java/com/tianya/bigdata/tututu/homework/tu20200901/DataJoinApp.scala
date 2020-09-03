package com.tianya.bigdata.tututu.homework.tu20200901

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataJoinApp {


  /**
   * 假设access.txt是小表，access.txt是大表,用broadcast把access.txt给广播出去
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val dataPath = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200901/data"
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)
    val textFileRDD: RDD[(String, String)] = sc.wholeTextFiles(dataPath).cache
    //使cache生效
    println(textFileRDD.count)
    //小表直接转map
    val accessMap: collection.Map[String, Access] = textFileRDD.filter(_._1.contains("access.txt")).map(tuple => {
      val splits: Array[String] = tuple._2.split(" ")
      (splits(0), Access(splits(0),splits(1),splits(2),splits(3)))
    }).collectAsMap()

    val broadcastMap: Broadcast[collection.Map[String, Access]] = sc.broadcast(accessMap)

    textFileRDD.filter(_._1.contains("user.txt")).map(user=>{
      val accessMaps: collection.Map[String, Access] = broadcastMap.value
      val splits: Array[String] = user._2.split(" ")
      val id = splits(0)
      val access: Access = accessMaps.getOrElse(id, Access(null, null, null, null))
      access
    })



    val tuples: Array[(String, String)] = textFileRDD.take(1)

    val tuple: (String, String) = tuples(0)

    println(tuple._1+":"+tuple._2)





    sc.stop()
  }

}


case class Access(id:String,year:String,month:String,traffic:String)
