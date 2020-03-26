package cn.spark.study.project.implicits

import org.apache.spark.rdd.RDD

/**
  * 自定义隐式转换切面
  */
object ImplicitAspect {

  implicit def rdd2RichRDD[T](rdd:RDD[T]) = new RichRDD[T](rdd)

}
