package com.tianya.bigdata.spark.day20200901

import org.apache.spark.util.AccumulatorV2

class MySumAccumulator extends AccumulatorV2[Int,Int]{
  private var sum = 0

  // 判断是否为0
  override def isZero: Boolean = sum == 0
  // 拷贝，把当前累加器赋值为一个新的累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val myNewAcc = new MySumAccumulator
    myNewAcc.sum = this.sum
    myNewAcc
  }
  //重置
  override def reset(): Unit = this.sum = 0

  //累加
  override def add(v: Int): Unit = sum += v

  //分区之间的累加器的合并
  override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
    case acc:MySumAccumulator => this.sum += acc.sum
    case _ => this.sum += 0
  }

  //返回累加器的值
  override def value: Int = sum
}
