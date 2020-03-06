package cn.spark.study.project

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._

object ProductInfoDataLoadTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ProductInfoDataLoadTest").enableHiveSupport().getOrCreate()
    val path = "hdfs://dse:8020/tmp/tianyafu/product_info.txt"
    //    val path = "hdfs://master:9000/tmp/tianyafu/user_visit_action.txt"
    val product_info_str: Dataset[String] = spark.read.textFile(path)
    val product_info_row:RDD[Row] = product_info_str.rdd.map(row =>{
      val splits: Array[String] = row.split("\\|")
      RowFactory.create(splits(0),splits(1),splits(2))
    })

    val structType = StructType(
      Array(
        StructField("product_id",StringType,true),
        StructField("product_name",StringType,true),
        StructField("extend_info",StringType,true)
      )
    )

    val product_info_df: DataFrame = spark.createDataFrame(product_info_row,structType)

    product_info_df.createTempView("product_info_temp")

    val sql = "insert overwrite table product_info select product_id,product_name,extend_info from product_info_temp"

    spark.sql(sql)
    product_info_df.show()

    spark.stop()

  }

}
