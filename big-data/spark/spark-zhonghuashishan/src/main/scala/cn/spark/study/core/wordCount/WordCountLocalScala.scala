package cn.spark.study.core.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocalScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCountLocal").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path: String = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark.txt"
    val lines: RDD[String] = sc.textFile(path)

    val wordCount: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wordCount.foreach(x => println(x._1 +" appeared " +x._2 +" times."))

  }

}
