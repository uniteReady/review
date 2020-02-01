package cn.spark.study.core.tranformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineCountScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local")

    val sc = new SparkContext(conf)

    val path = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\hello.txt"

    val linesRDD: RDD[String] = sc.textFile(path)

    val resultRDD: RDD[(String, Int)] = linesRDD.map((_, 1)).reduceByKey(_ + _)

    resultRDD.foreach(result => println(result._1 + " appeared " + result._2 + " times."))

    sc.stop()
  }

}
