package cn.spark.study.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UDF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("UDF").getOrCreate()

    val names = Array("leo","Marry","Tom")
    val namesRDD: RDD[Row] = spark.sparkContext.parallelize(names).map(name => Row(name))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val namesDF: DataFrame = spark.createDataFrame(namesRDD,structType)

    namesDF.createTempView("names_table")
    spark.udf.register("strLen",(x:String) =>x.length)
    spark.sql("select name ,strLen(name) from names_table").show()

    spark.stop()
  }

}
