package cn.spark.study.project


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object UserInfoDataLoadTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
//      .master("spark://master:7077")
      .appName("UserInfoDataLoadTest")
      .enableHiveSupport()
      .getOrCreate()
//    val path = "hdfs://dse:8020/tmp/tianyafu/user_info.txt"
    val path = "hdfs://master:9000/tmp/tianyafu/user_info.txt"
//    val path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-bussiness-data-analysis-zhonghuashishan\\src\\main\\resources\\data\\user_info.txt"
    val user_info_str: Dataset[String] = spark.read.textFile(path)
    val user_info_row:RDD[Row] = user_info_str.rdd.map(row =>{
      val splits: Array[String] = row.split("\\|")
      RowFactory.create(splits(0),splits(1),splits(2),splits(3),splits(4),splits(5),splits(6))
    })
    val structType = StructType(
      Array(
        StructField("user_id",StringType,true),
        StructField("username",StringType,true),
        StructField("name",StringType,true),
        StructField("age",StringType,true),
        StructField("professional",StringType,true),
        StructField("city",StringType,true),
        StructField("sex",StringType,true)
      )
    )
    val user_info_df: DataFrame = spark.createDataFrame(user_info_row,structType)
    user_info_df.createTempView("user_info_temp")
    spark.sql("insert overwrite table user_info select user_id,username,name,age,professional,city,sex from user_info_temp")
    user_info_df.show()
    spark.stop()
  }

}

