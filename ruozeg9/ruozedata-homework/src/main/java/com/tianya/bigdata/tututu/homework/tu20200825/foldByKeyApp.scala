package com.tianya.bigdata.tututu.homework.tu20200825

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey算子传入一个初值zeroValue和一个函数fun
 * 从foldByKey中fun定义来看，是从(v,v) => v,也就是返回的和输入的是同一个类型，不像combineByKey那样可以是不同的类型
 * 底层调用的是combineByKeyWithClassTag算子
 * createCombiner函数是将给定的初值作用于每个分区的key的第一个value上，对应源码中，cleanedFunc(createZero(), v)
 * mergeValue函数是fun，将经过createCombiner计算的值所谓zeroValue，对每个分区的每种key的value进行计算，上一次的计算结果作为下一个的zeroValue
 * mergeCombiners函数是fun
 */
object foldByKeyApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2), ("B", 3), ("B", 4), ("C", 1)))
    rdd1.foldByKey(8,1)(
      (x: Int,y:Int) => {
        println("fun函数"+x+"|"+y)
        x+y
      }
    ).collect.foreach(println)
    sc.stop()
  }
}
