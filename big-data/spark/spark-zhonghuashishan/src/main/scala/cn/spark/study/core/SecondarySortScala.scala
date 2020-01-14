package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SecondarySortScala")

    val sc = new SparkContext(conf)

    val path: String = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\sort.txt"
    val lineRDD: RDD[String] = sc.textFile(path)

    val pairRDD = lineRDD.map(x =>{
      val splits: Array[String] = x.split(" ")
      val key:SecondarySortKeyScala = new SecondarySortKeyScala(splits(0).toInt,splits(1).toInt)
      (key,x)
    })

    pairRDD.sortByKey().map(x => x._2).foreach(println)

    sc.stop()
  }

}
