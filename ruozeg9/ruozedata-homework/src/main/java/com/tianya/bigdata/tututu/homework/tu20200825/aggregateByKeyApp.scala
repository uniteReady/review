package com.tianya.bigdata.tututu.homework.tu20200825

/**
 * aggregateByKey算子的demo
 * 根据aggregateByKey算子的源码，底层调用的是combineByKeyWithClassTag
 * createCombiner函数是为每个key生成初值的函数，只不过这个的初值就是调用aggregateByKey时给的初值
 * mergeValue函数就是seqOp函数
 * mergeCombiners函数就是combOp函数
 *
 */
object aggregateByKeyApp {


  def main(args: Array[String]): Unit = {



  }

}



