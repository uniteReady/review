package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Accumulator")

    val sc = new SparkContext(conf)

    val sumAccumulator: LongAccumulator = sc.longAccumulator("sumAccumulator")

    val numsRDD: RDD[Long] = sc.parallelize(List(1L,2L,3L,4L,5L))

    numsRDD.foreach(x => sumAccumulator.add(x))
    println(sumAccumulator.value)

    sc.stop()
  }

}
