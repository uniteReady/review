package cn.spark.study.core.tranformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollectionScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ParallelizeCollectionScala").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numRDD: RDD[Int] = sc.parallelize(nums)

    val result: Int = numRDD.reduce(_ + _)

    println("1到10的总和为:" + result)


    sc.stop()
  }

}
