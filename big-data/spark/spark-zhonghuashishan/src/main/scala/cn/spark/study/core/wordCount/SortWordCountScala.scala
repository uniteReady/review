package cn.spark.study.core.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortWordCountScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sortWordCount")

    val sc = new SparkContext(conf)

    val path : String = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\hello.txt"
    val linesRDD: RDD[String] = sc.textFile(path)

    linesRDD.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x =>(x._2,x._1))
      .foreach(x => println( "word: "+ x._1 + "，count: "+ x._2))

    sc.stop()
  }

}
