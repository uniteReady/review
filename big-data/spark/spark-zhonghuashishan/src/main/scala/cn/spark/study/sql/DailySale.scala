package cn.spark.study.sql

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object DailySale {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DailySale").master("local").getOrCreate()

    import spark.implicits._

    val saleLog= Array(
      "2020-02-07,55.05",
      "2020-02-07,56.05",
      "2020-02-07,55.05",
      "2020-02-07,57.05",
      "2020-02-08,50.05",
      "2020-02-08,15.05",
      "2020-02-08,52.05"
    )
    val userSaleRDD = spark.sparkContext.parallelize(saleLog).map(
      x =>{
        val splits: Array[String] = x.split(",")
        Row(splits(0),splits(1).toDouble)
      }
    )
    val structType = StructType(
      Array(
        StructField("date",StringType,true),
        StructField("sale",DoubleType,true)
      )
    )

    val userSaleDF: DataFrame = spark.createDataFrame(userSaleRDD,structType)

    userSaleDF
      .groupBy("date")
      .agg('date,sum('sale))
      .map(row =>Sale(row(1).toString,row(2).toString.toDouble))
      .collect()
      .foreach(println)


    spark.stop()

  }

}

case class Sale(date:String ,saleSum:Double)
