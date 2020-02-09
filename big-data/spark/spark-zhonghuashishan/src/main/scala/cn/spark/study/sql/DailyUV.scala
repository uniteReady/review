package cn.spark.study.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


/**
  * 根据每日的购买日志和销售日志，统计uv和销售额
  */
object DailyUV {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("DailyUV").getOrCreate()
    import spark.implicits._
    //构造用户访问日志数据,日志用逗号隔开，第一列是日期，第二列是用户id，并创建DataFrame
    val userAccessLog = Array(
      "2020-02-07,1122",
      "2020-02-07,1122",
      "2020-02-07,1123",
      "2020-02-07,1124",
      "2020-02-07,1124",
      "2020-02-08,1122",
      "2020-02-08,1121",
      "2020-02-08,1123",
      "2020-02-08,1123"
    )
    //将用户访问日志转换为RDD[Row]的形式
    val rowRDD = spark.sparkContext.parallelize(userAccessLog).map(
      x => {
        val splits: Array[String] = x.split(",")
        Row(splits(0), splits(1).toInt)
      }
    )
    //构造DataFrame的元数据
    val structType = StructType(
      Array(StructField("date", StringType, true),
        StructField("userId", IntegerType, true))
    )

    //使用sparkSession创建DataFrame
    val userAccessLogDF: DataFrame = spark.createDataFrame(rowRDD, structType)

    /**
      * 讲解一下聚合函数的用法
      * 首先，对DataFrame调用groupBy方法，对某一列进行分组，然后调用agg()方法，第一个参数必须传入groupBy方法中出现的字段
      * 第二个参数，传入countDistinct、sum、first等spark提供的内置函数，内置函数中，传入的参数，也是用单引号作为前缀的其它字段
      */
    userAccessLogDF
      .groupBy("date")
      .agg('date, countDistinct('userId))
      .map(row => UV(row(1).toString,row(2).toString.toInt))
      .collect()
      .foreach(println)


    spark.stop()
  }

}

case class UV(date:String,uv:Int)
