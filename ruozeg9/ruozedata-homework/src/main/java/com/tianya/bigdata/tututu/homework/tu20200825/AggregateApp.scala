package com.tianya.bigdata.tututu.homework.tu20200825

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateApp {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(conf)

    val numRDD= sc.parallelize(1 to 4, 2)

    val fun1: (Int, Int) => Int = _ + _
    val fun2: (Int, Int) => Int = _ * _

    /**
     * 1个分区的时候
     * fun1
     * 4 + 1 = 5
     * 5 + 2 = 7
     * 7 + 3 = 10
     * 10 + 4 = 14
     *
     * fun2
     * 4 * 14 = 56
     *
     * 2个分区的时候
     * fun1
     * 4 + 1 + 2 = 7
     * 4 + 3 + 4 = 11
     *
     * fun2
     * 4 * 7 * 11 = 308
     *
     * 总结：
     * zeroValue是初始值
     * fun1会作用在每一个分区的数据上，初始值为 zeroValue 得出每一个分区的计算结果
     * 然后以zeroValue为初值，将fun2作用在每一个上面得到的分区计算结果
     *
     */
    val result: Int = numRDD.aggregate(4)(fun1, fun2)
    println("结果是:"+result)




    sc.stop()
  }

}
