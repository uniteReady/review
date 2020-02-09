package cn.spark.study.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UDAF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("UDAF").getOrCreate()

    val names = Array("leo","Marry","Tom","Marry","Marry")
    val namesRDD: RDD[Row] = spark.sparkContext.parallelize(names).map(name => Row(name))
    val structType = StructType(Array(StructField("name",StringType,true)))
    val namesDF: DataFrame = spark.createDataFrame(namesRDD,structType)

    namesDF.createTempView("names_table")
    spark.udf.register("strCount",new StringCountUDAF)
    spark.sql("select name ,strCount(name) from names_table group by name").show()

    spark.stop()
  }

}
