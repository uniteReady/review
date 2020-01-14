package cn.spark.study.core.tranformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TranformationOperationScala {

  def main(args: Array[String]): Unit = {
    //    map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortBykey()
//    join()
    cogroup()
  }

  def cogroup():Unit={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("cogroup")

    val sc = new SparkContext(conf)
    val studentsRDD = sc.parallelize(
      List(
        Tuple2(1,"leo"),
        Tuple2(2,"jack"),
        Tuple2(3,"tom")
      )
    )

    val scoreRDD = sc.parallelize(
      List(
        Tuple2(1,100),
        Tuple2(2,70),
        Tuple2(2,80),
        Tuple2(3,70),
        Tuple2(1,90),
        Tuple2(3,70)
      )
    )

    val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = studentsRDD.cogroup(scoreRDD)

    cogroupRDD.foreach(
      x =>{
        println("student id :"+ x._1)
        println("student name : " + x._2._1)
        println("student score : "+ x._2._2)
        println("===================")
      }
    )


    sc.stop()
  }

  def join():Unit={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("join")

    val sc = new SparkContext(conf)

    val studentsRDD = sc.parallelize(
      List(
        Tuple2(1,"leo"),
        Tuple2(2,"jack"),
        Tuple2(3,"tom")
      )
    )

    val scoreRDD = sc.parallelize(
      List(
        Tuple2(1,100),
        Tuple2(2,70),
        Tuple2(2,80),
        Tuple2(3,70),
        Tuple2(1,90),
        Tuple2(3,70)
      )
    )
    val joinRDD: RDD[(Int, (String, Int))] = studentsRDD.join(scoreRDD)

    joinRDD.foreach(
      x => {
        println("student id :" + x._1)
        println("student name :" + x._2._1)
        println("student score: " + x._2._2 )
        println("============================")
      }
    )

    sc.stop()
  }


  def sortBykey():Unit={

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduceByKey")

    val sc = new SparkContext(conf)

    val scores = sc.parallelize(Array(
      new Tuple2[Integer,String](80,"tom"),
      new Tuple2[Integer,String](65,"jerry"),
      new Tuple2[Integer,String](90,"marry"),
      new Tuple2[Integer,String](80,"susam")
    ))

    scores.sortByKey(false).foreach(println)


    sc.stop()

  }

  def reduceByKey():Unit={
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduceByKey")

    val sc = new SparkContext(conf)

    val scores = sc.parallelize(Array(
      new Tuple2[String,Integer]("class1",80),
      new Tuple2[String,Integer]("class2",65),
      new Tuple2[String,Integer]("class2",90),
      new Tuple2[String,Integer]("class1",80)
    ))

    val resultRDD: RDD[(String, Integer)] = scores.reduceByKey(_+_)

    resultRDD.foreach(x => println("class: "+ x._1 +" ,scoreSum: "+x._2))

    sc.stop()
  }

  def groupByKey():Unit={
    val conf: SparkConf = new SparkConf().setAppName("TranformationOperationScala").setMaster("local")

    val sc = new SparkContext(conf)

    val scores = sc.parallelize(Array(
      new Tuple2[String,Integer]("class1",80),
      new Tuple2[String,Integer]("class2",65),
      new Tuple2[String,Integer]("class2",90),
      new Tuple2[String,Integer]("class1",80)
    ))

    val groupedScore: RDD[(String, Iterable[Integer])] = scores.groupByKey()

    groupedScore.foreach(
      x => {
        println("class: "+x._1)
        val it: Iterator[Integer] = x._2.iterator

        while (it.hasNext){
          println(it.next())
        }
      }

    )

    sc.stop()
  }

  def flatMap():Unit={
    val conf: SparkConf = new SparkConf().setAppName("TranformationOperationScala").setMaster("local")

    val sc = new SparkContext(conf)

    val text:String = "hello world hello spark hello hadoop hello elasticsearch"
    val textRDD: RDD[String] = sc.parallelize(Array(text))

    textRDD.flatMap(_.split(" ")).foreach(println)

    sc.stop()
  }

  def filter(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TranformationOperationScala").setMaster("local")

    val sc = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    numRDD.filter(_ % 2 == 0).foreach(println)

    sc.stop()
  }

  def map(): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TranformationOperationScala").setMaster("local")

    val sc = new SparkContext(conf)

    val nums = Array(1, 2, 3, 4, 5)

    val numsRDD: RDD[Int] = sc.parallelize(nums)

    numsRDD.map(_ * 2).foreach(println)

    sc.stop()
  }

}
