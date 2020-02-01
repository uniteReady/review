package cn.spark.study.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameCreateScala {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("DataFrameCreateScala").getOrCreate()


    val path: String = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.json"
    val jsonDF: DataFrame = spark.read.json(path)

    jsonDF.show()


    spark.stop()
  }

}
