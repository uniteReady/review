package cn.spark.study.sql.typeOperation

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TypeOperation {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("TypeOperation").master("local").getOrCreate()

    import spark.implicits._
    val path = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\people.json"
    val peopleDF: DataFrame = spark.read.json(path )
    peopleDF.show()

    //该方法中getAs() 如果age为null的话 会转成0 看getAs()源码的注释即可
    val peopleDS: Dataset[People] = peopleDF.map(row => People(row.getAs[String](1),row.getAs[Long](0)))
    peopleDS.show()

    val peopleDS2: Dataset[People] = peopleDF.as[People]
    peopleDS2.show()







    spark.stop()
  }

}


case class People(name : String , age : Long)
