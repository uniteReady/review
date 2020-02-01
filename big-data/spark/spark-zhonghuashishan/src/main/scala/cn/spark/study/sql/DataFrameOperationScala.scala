package cn.spark.study.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataFrameOperationScala {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataFrameOperationScala").master("local").getOrCreate()

    import spark.implicits._

    val path:String = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.json"
    val jsonDF: DataFrame = spark.read.json(path)
    //打印出df的前20行
    jsonDF.show()
    //打印出元数据信息
    jsonDF.printSchema()

    jsonDF.select("name","age").show()


    //转换为Dataset
    val studentDS: Dataset[StudentScala] = jsonDF.as[StudentScala]

    studentDS.printSchema()





    spark.stop()
  }

}

case class  StudentScala(id:Long,name:String,age:Long)
