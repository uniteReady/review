package cn.spark.study

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object RDDToDF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("RDDToDF").master("local").getOrCreate()

    //导入隐式转换
    import spark.implicits._

    //创建RDD
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))
    //将RDD[Int]转换为RDD[Row]

    val rowRDD: RDD[Row] = rdd.map(x => Row(x))

    //创建结构信息
    val structType = StructType(Array(StructField("id", IntegerType)))

    //创建DF
    val df: DataFrame = spark.createDataFrame(rowRDD, structType)

    df.show()


    spark.stop()
  }

}
