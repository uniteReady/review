package com.tianya.bigdata.tututu.homework.tu20200825

import org.apache.spark.{SparkConf, SparkContext}

/**
 * aggregateByKey算子的demo
 * 根据aggregateByKey算子的源码，底层调用的是combineByKeyWithClassTag
 * createCombiner函数是作用在每个分区的每个key上的，一个分区的相同的key只执行一次，
 * 就是一个把(zeroValue,V) =>U的过程 zeroValue和U类型相同
 * mergeValue函数就是seqOp函数
 * mergeCombiners函数就是combOp函数,将不同分区中相同的key经过mergeValue计算后的值进行combOp函数计算
 *
 */
object aggregateByKeyApp {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2),("B",3),("B",4), ("C", 1)))
    rdd1.aggregateByKey("zeroValue")(
      (u: String,v:Int) => {
        println("seqOp函数:"+u +":"+ v)
        u + "_" + v
      },
      (u: String, u2: String) => {
        println("combOp函数"+u+":"+u2)
        u + "@" + u2
      }
    ).collect.foreach(println)
    sc.stop()
  }
}



