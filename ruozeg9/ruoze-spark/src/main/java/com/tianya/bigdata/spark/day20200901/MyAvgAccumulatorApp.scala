package com.tianya.bigdata.spark.day20200901

import org.apache.spark.util.AccumulatorV2


class MyAvgAccumulatorApp extends AccumulatorV2[Double, Map[String, Any]] {

  private var myMap = Map[String, Any]()

  override def isZero: Boolean = myMap.isEmpty

  override def copy(): AccumulatorV2[Double, Map[String, Any]] = {
    println("..........copy..............")
    val myNewAcc = new MyAvgAccumulatorApp
    myNewAcc.myMap = this.myMap
    myNewAcc
  }

  override def reset(): Unit = Map[String, Any]()

  override def add(v: Double): Unit = {
    myMap += "sum" -> (myMap.getOrElse("sum", 0D).asInstanceOf[Double] + v)
    myMap += "count" -> (myMap.getOrElse("count", 0D).asInstanceOf[Double] + 1D)
  }

  override def merge(other: AccumulatorV2[Double, Map[String, Any]]): Unit = {
    other match {
      case o: MyAvgAccumulatorApp => {
        myMap += "sum" -> (this.myMap.getOrElse("sum", 0D).asInstanceOf[Double] + o.myMap.getOrElse("sum", 0D).asInstanceOf[Double])
        myMap += "count" -> (this.myMap.getOrElse("count", 0D).asInstanceOf[Double] + o.myMap.getOrElse("count", 0D).asInstanceOf[Double])
      }
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Any] = {
    myMap += "avg" -> (myMap.getOrElse("sum", 0D).asInstanceOf[Double] / myMap.getOrElse("count", 0D).asInstanceOf[Double])
    myMap
  }
}
