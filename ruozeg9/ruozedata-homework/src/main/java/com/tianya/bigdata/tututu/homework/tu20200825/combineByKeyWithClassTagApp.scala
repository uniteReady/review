package com.tianya.bigdata.tututu.homework.tu20200825

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 参考 https://www.jianshu.com/p/0c6705724cff
 * combineByKeyWithClassTag中最主要的3个参数为
 * createCombiner：每个分区中每个key的第一个value会运行这个函数，从v 变成 c
 * mergeValue：每个分区中每个key的第二个value开始，依次运行这个函数，上一次运行得到的c与当前value进行运算；
 * 对于第二个value来说，上一次运行得到的c就是经过createCombiner运算的c
 * mergeCombiners：将不同分区中的相同的key所对应的value进行运算
 *
 */
object combineByKeyWithClassTagApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2),("B",3),("B",4), ("C", 1)))
    rdd1.combineByKeyWithClassTag(
      (v: Int) => {
        println("createCombiner函数"+v)
        v + "_"
      },
      (c: String, v: Int) => {
        println("mergeValue函数"+c+":"+v)
        c + "@" + v
      },
      (c1: String, c2: String) => {
        println("mergeCombiners函数"+c1+":"+c2)
        c1 + "$" + c2
      }
    ).collect.foreach(println)
    sc.stop()
  }
}
