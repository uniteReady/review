package cn.spark.study.project.implicits

import org.apache.spark.rdd.RDD

class RichRDD[T](var rdd:RDD[T]){

  def printInfo(flag:Int = 0): Unit ={
    if(flag == 0){
      rdd.collect().foreach(println)
      println(".................华丽的分割线.......................")
    }
  }

}
