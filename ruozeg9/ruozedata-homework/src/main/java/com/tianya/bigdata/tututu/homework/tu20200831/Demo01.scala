package com.tianya.bigdata.tututu.homework.tu20200831

import com.tianya.bigdata.homework.day20200801.FileUtils
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Demo01 {

  def main(args: Array[String]): Unit = {

    val out = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200831/out"

    //  1 ，spark sql 上下文
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .appName("SparkDemoFromS3")
      .getOrCreate()
    //  2 ，设置日志级别 ：
    spark.sparkContext.setLogLevel("ERROR")

    FileUtils.delete(spark.sparkContext.hadoopConfiguration,out)
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._
    //  3 ，读文件 ：
    val dfYuan: DataFrame = spark.read
      .option("header","true")
      .option("delimiter",",")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv("ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200831/students.csv")
    //  4 ，转换 ： 指定列名
    val dfTable: DataFrame = dfYuan.toDF("id","name","age","sex")
    //  注册表
    dfTable.createOrReplaceTempView("students")
    //  5 ，转换 ： 产生 yearmonth 列 ( 共 12 列 )
    val saleYm: DataFrame = spark.sql("select id,name,age,sex from students order by id")
    //  6 ，转换为 rdd ( 年月-其他所有 )
    val rddRes: RDD[(String, String)] = saleYm.rdd.map(row => {
      val k: String = row.get(0).toString
      val v: String = row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3)
      (k, v)
    })
    //  7 ，输出
    rddRes.saveAsHadoopFile(out,classOf[String], classOf[String],classOf[RDDMultipleTextOutputFormat1])
    //  8 ，释放资源
    spark.close()
  }

}

//  指定文件名( 根据 k-v 指定文件名 )
class RDDMultipleTextOutputFormat1 extends MultipleTextOutputFormat[String, String] {
  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = key+".log"
}