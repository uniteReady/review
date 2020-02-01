package cn.spark.study.core.topN

import org.apache.spark.{SparkConf, SparkContext}

object Top3Scala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("top3Scala")

    val sc = new SparkContext(conf)

    val path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\top.txt"
    sc.textFile(path)
      .map(x => (x.toInt, 1))
      .sortByKey(false)
      .map(x => x._1)
      .take(3)
      .foreach(println)

    sc.stop()
  }


}
