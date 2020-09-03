package com.tianya.bigdata.tututu.homework.tu20200831

import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sparkCdnETLApp {


  /**
   *
   * @param log
   * @return (time,domain,path,traffic)
   */
  def getDomainFromUrl(log: String): (String, String, String, Long) = {
    val splits: Array[String] = log.split("\t")
    val time = splits(0).substring(1, splits(0).length - 1)
    val url: String = splits(6)
    val responseSize: String = splits(9)
    val traffic: Long = if ("-" == responseSize) -1L else responseSize.toLong
    val urlSplits = url.split("\\?")
    val urlSplits2 = urlSplits(0).split(":")

    //    val http = urlSplits2(0)
    val urlSpliting = urlSplits2(1).substring(2)
    var domain = urlSpliting
    var path = ""
    if (urlSpliting.contains("/")) {
      domain = urlSpliting.substring(0, urlSpliting.indexOf("/"))
      path = urlSpliting.substring(urlSpliting.indexOf("/"))
    }
    (time, domain, path, traffic)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val in = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200831/cdn.log"
    val out1 = "out/ruozedataLog"
    val out2 = "out/ruozekeqqLog"

    val fileRDD: RDD[String] = sc.textFile(in)
    //    fileRDD.cache

    val tupleRDD: RDD[(String, String, String, Long)] = fileRDD.map(getDomainFromUrl(_)).filter(_._4 != -1L)

    tupleRDD.cache
    //使cache生效
    println("输入的数据去脏后的条数:" + tupleRDD.count())

    tupleRDD.filter("www.ruozedata.com" == _._1).map(x => (x._1,x._2,x._4)).saveAsTextFile(out1,classOf[GzipCodec])
    tupleRDD.filter("ruoze.ke.qq.com" == _._1).saveAsTextFile(out2,classOf[BZip2Codec])


    sc.stop()
  }

}