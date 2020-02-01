package cn.spark.study

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("wordCount").master("local").getOrCreate()

    //导入隐式转换
    import spark.implicits._

    //读取文件，创建df
    val path: String = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-atguigu\\src\\main\\resources\\data\\people.json"
    val peopleDF: DataFrame = spark.read.json(path)

    //DSL风格的
    peopleDF.select("name").show()

    //SQL风格的
    peopleDF.createTempView("people")
    spark.sql("select name from people").show()

    //df 转RDD
    val peopleRDD: RDD[Row] = peopleDF.rdd
    val mapRDD: RDD[(String, Long)] = peopleRDD.map(row => (row.getAs[String]("name"), row.getAs[Long]("age")))
    println("********************")
    mapRDD.foreach(x => println("name is : " + x._1 + " , age is : " + x._2))
    println("********************")


    //df 转换为 DataSet
    val peopelDS: Dataset[People] = peopleDF.as[People]
    peopelDS.show()
    peopelDS.printSchema()

    spark.stop()


  }

}

case class People(name: String, age: Long)
