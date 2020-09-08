package com.tianya.bigdata.homework.day20200908

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * exists
 * 参考http://spark.apache.org/docs/2.4.6/api/sql/index.html#exists
 * in
 * 参考http://spark.apache.org/docs/2.4.6/api/sql/index.html#in
 *
 * [not] exists、[not] in的效率
 * exists using LEFT SEMI joins
 * not Exists using LEFT ANTI joins
 * in using LEFT SEMI joins
 * not in is a very expensive operator in practice
 * 从join的类型的角度来看，exists和 not exists 和 in 都是相同的join (left anti join与 left semi join类似)
 * 从mysql和oracle的经验来看  in是不走索引 所以 exists和 not exists的效率要高于 in 和 not in
 * 参考https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html
 * join的类型参考https://www.cnblogs.com/suanec/p/7560399.html
 */
object ExistsVSInApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("").master("local[2]").getOrCreate()
    val students = List((1,"pk", 30),(2,"ruoze", 31),(3,"xingxing", 18),(4,"j哥", 20))
    val scores = List((1,60),(1,61),(3,62),(4,63))

    val sc: SparkContext = spark.sparkContext

    val studentRDD: RDD[Row] = sc.parallelize(students).map(x => Row(x._1, x._2, x._3))
    val scoreRDD: RDD[Row] = sc.parallelize(scores).map(x => Row(x._1, x._2))

    val studentDF =  spark.createDataFrame(studentRDD,StructType(List(
      StructField("id",DataTypes.IntegerType,true),
      StructField("name",DataTypes.StringType,true),
      StructField("age",DataTypes.IntegerType,true)
    )))

    val scoreDF =  spark.createDataFrame(scoreRDD,StructType(List(
      StructField("id",DataTypes.IntegerType,true),
      StructField("score",DataTypes.IntegerType,true)
    )))

    studentDF.createOrReplaceTempView("students")
    scoreDF.createOrReplaceTempView("scores")

    val existsSql = "select a.id , a.name, a.age from students a where exists (select b.id from scores b where a.id = b.id)"
    val notExistsSql = "select a.id , a.name, a.age from students a where not exists (select b.id from scores b where a.id = b.id)"
    val inSql = "select a.id , a.name, a.age from students a where a.id in (select b.id from scores b)"
    val notInSql = "select a.id , a.name, a.age from students a where a.id not in (select b.id from scores b)"

    spark.sql(existsSql).show()
    spark.sql(notExistsSql).show()
    spark.sql(inSql).show()
    spark.sql(notInSql).show()


    spark.stop()
  }

}