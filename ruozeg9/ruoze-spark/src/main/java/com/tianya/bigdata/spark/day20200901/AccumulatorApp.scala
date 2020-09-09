package com.tianya.bigdata.spark.day20200901

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,3,5,6,7,8),3)

    val cntsAccumulator: LongAccumulator = sc.longAccumulator("cnts")

    rdd.foreach(x => {
      cntsAccumulator.add(1)
    })

    println(cntsAccumulator.value)

    /*
    //这么写会有闭包的问题
    var cnts = 0

    rdd.foreach(x => {
      cnts +=1
      println(s"....$cnts")
    })

    println(cnts)*/

    val userAccumulator: CollectionAccumulator[User] = sc.collectionAccumulator[User]("用户信息累加器")

    val usersRDD: RDD[User] = sc.parallelize(Array(
      User("pk", "13758586555"),
      User("ruoze", "13758586535"),
      User("xingxing", "13758586777")
    ))

    usersRDD.foreach(user=>{
      val phone: String = user.phone.reverse
      if(phone(0) == phone(1) && phone(1) == phone(2)){
        userAccumulator.add(user)
      }
    })

    println(userAccumulator.value)

    val rdd2 = sc.parallelize(List(10,20,30,40,50))

    val myAcc = new MySumAccumulator
    sc.register(myAcc,"我自己的累加器")

    rdd2.map(x => {
      myAcc.add(x)
      x
    }).foreach(println)

    println("我自己的累加器:" + myAcc.value)


    val rdd3 = sc.parallelize(List(50,30,60,10,20,30,40,50),3)
    val myAvgAccumulatorApp = new MyAvgAccumulatorApp
    sc.register(myAvgAccumulatorApp,"我自己定义的求平均值的累加器")
    rdd3.foreach(x => {
      myAvgAccumulatorApp.add(x)
    })

    println(myAvgAccumulatorApp.value)




    sc.stop()
  }

}

case class User(name:String,phone:String)
