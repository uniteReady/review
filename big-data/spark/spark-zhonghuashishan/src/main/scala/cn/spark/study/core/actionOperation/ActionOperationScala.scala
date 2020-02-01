package cn.spark.study.core.actionOperation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOperationScala {

  def main(args: Array[String]): Unit = {
    //    reduce()
    //    collect()
    //    count()
    //take()
    //saveAsTextFile()
    countByKey()
  }

  def countByKey(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("saveAsTextFile")
    val sc = new SparkContext(conf)

    val classStudentRDD = sc.parallelize(List(
      Tuple2("class1", "leo"),
      Tuple2("class3", "sam"),
      Tuple2("class3", "jerry"),
      Tuple2("class3", "lilei"),
      Tuple2("class3", "marry"),
      Tuple2("class3", "jack")
    ))

    val classCountMap: collection.Map[String, Long] = classStudentRDD.countByKey()

    classCountMap.foreach(
      x => {
        println("class: " + x._1 + ", studentCount: " + x._2)
      }
    )


    sc.stop()
  }

  def saveAsTextFile(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("saveAsTextFile")
    val sc = new SparkContext(conf)
    val numsRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\saveAsTextFileJava.txt";
    numsRDD.map(_ * 2).saveAsTextFile(path)

    sc.stop()
  }

  def take(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("take")
    val sc = new SparkContext(conf)
    val numsRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val ints: Array[Int] = numsRDD.take(3)

    ints.foreach(println)

    sc.stop()
  }

  def count(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("count")
    val sc = new SparkContext(conf)
    val numsRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val count: Long = numsRDD.count()

    println(count)

    sc.stop()
  }

  def collect(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("collect")
    val sc = new SparkContext(conf)
    val numsRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val resultList: Array[Int] = numsRDD.map(_ * 2).collect()

    resultList.foreach(println)

    sc.stop()
  }

  def reduce(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
    val numsRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    val result: Int = numsRDD.reduce(_ + _)

    println(result)


    sc.stop()
  }


}
