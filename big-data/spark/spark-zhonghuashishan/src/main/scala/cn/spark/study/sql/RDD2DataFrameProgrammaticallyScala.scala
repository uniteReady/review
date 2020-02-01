package cn.spark.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DataFrameProgrammaticallyScala {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataFrameOperationScala").master("local").getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val path: String = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.txt"

    val linesRDD: RDD[String] = sc.textFile(path)

    val studentsRDD=linesRDD.map(
      line =>{
        val splits: Array[String] = line.split(",")
        Row(splits(0).toString.trim.toInt,splits(1),splits(2).toString.trim.toInt)
      }
    )

    val structType = StructType(Array(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("age",IntegerType,true)))

    val df: DataFrame = spark.createDataFrame(studentsRDD,structType)

    df.createTempView("student3")

    val teenagerDF: DataFrame = spark.sql("select * from student3 where age >=18")

    teenagerDF.show()

    spark.stop()

  }

}
