package cn.spark.study.project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object UserVisitActionDataLoadTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("UserVisitActionDataLoadTest").enableHiveSupport().getOrCreate()
    val path = "hdfs://dse:8020/tmp/tianyafu/user_visit_action.txt"
//    val path = "hdfs://master:9000/tmp/tianyafu/user_visit_action.txt"
    val user_visit_action_str: Dataset[String] = spark.read.textFile(path)
    val user_visit_action_row:RDD[Row] = user_visit_action_str.rdd.map(row =>{
      val splits: Array[String] = row.split("\\|")
      RowFactory.create(splits(0),splits(1),splits(2),splits(3),splits(4),splits(5),splits(6),splits(7),splits(8),splits(9),splits(10),splits(11),splits(12))
    })


    val structType = StructType(
      Array(
        StructField("date",StringType,true),
        StructField("user_id",StringType,true),
        StructField("session_id",StringType,true),
        StructField("page_id",StringType,true),
        StructField("action_time",StringType,true),
        StructField("search_keyword",StringType,true),
        StructField("click_category_id",StringType,true),
        StructField("click_product_id",StringType,true),
        StructField("order_category_ids",StringType,true),
        StructField("order_product_ids",StringType,true),
        StructField("pay_category_ids",StringType,true),
        StructField("pay_product_ids",StringType,true),
        StructField("city_id",StringType,true)
      )
    )
    val user_visit_action_df: DataFrame = spark.createDataFrame(user_visit_action_row,structType)
    user_visit_action_df.select("action_time").show()
    println(user_visit_action_df.rdd.take(1))
    user_visit_action_df.createTempView("user_visit_action_temp")
    spark.sql("insert overwrite table user_visit_action " +
      "select " +
      "date," +
      "user_id," +
      "session_id," +
      "page_id," +
      "action_time," +
      "search_keyword," +
      "click_category_id," +
      "click_product_id," +
      "order_category_ids," +
      " order_product_ids," +
      "pay_category_ids," +
      "pay_product_ids, " +
      "city_id " +
      "from user_visit_action_temp")
    user_visit_action_df.show()
    spark.stop()
  }

}
