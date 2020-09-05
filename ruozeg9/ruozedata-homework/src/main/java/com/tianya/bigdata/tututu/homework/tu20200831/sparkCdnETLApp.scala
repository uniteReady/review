package com.tianya.bigdata.tututu.homework.tu20200831

import com.tianya.bigdata.homework.day20200801.FileUtils
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 0831作业，要将数据输出到某个文件中，要自定义OutputFormat
 */
object sparkCdnETLApp {

  /**
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

    FileUtils.delete(sc.hadoopConfiguration, out1)
    FileUtils.delete(sc.hadoopConfiguration, out2)

    val fileRDD: RDD[String] = sc.textFile(in)
    val tupleRDD: RDD[(String, String, String, Long)] = fileRDD
      .map(getDomainFromUrl(_))
      .filter(_._4 != -1L)
      .cache
    //使cache生效
    println("输入的数据去脏后的条数:" + tupleRDD.count())

    val qqRDD: RDD[(String, String)] = tupleRDD.filter(x => {
      "ruoze.ke.qq.com" == x._2
    }).map(x => (x._2, RuozeKeQQ(x._1, x._2, x._3, x._4).toString))

    val ruozedataRDD: RDD[(String, String)] = tupleRDD.filter(x => {
      "www.ruozedata.com" == x._2
    }).map(x => (x._2, RuozeData(x._1, x._2, x._4).toString))

    qqRDD
      .coalesce(1) // 不减分区会少数据
      .saveAsHadoopFile(out2, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat], classOf[BZip2Codec])

    ruozedataRDD
      .coalesce(1) // 不减分区会少数据
      .saveAsHadoopFile(out1, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat], classOf[GzipCodec])

    sc.stop()
  }

}

case class RuozeData(time: String, domain: String, traffic: Long) {
  override def toString: String = time + "\t" + traffic
}

case class RuozeKeQQ(time: String, domain: String, path: String, traffic: Long) {
  override def toString: String = time + "\t" + path + "\t" + traffic
}

class MyMultipleTextOutputFormat extends MultipleTextOutputFormat[String, String] {
  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = {
    if (key == "ruoze.ke.qq.com") {
      "ruozekeqq.log"
    } else if (key == "www.ruozedata.com") {
      "ruozedata.log"
    } else {
      key + ".log"
    }
  }
}