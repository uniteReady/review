package cn.spark.study.sql.untypeOperation

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkSqlDemo")
//      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val path = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\people.json"
    val peopleDS: DataFrame = spark.read.json(path)

    peopleDS.show()
    peopleDS.printSchema()
    peopleDS.select("name").show()//select 操作,典型的弱类型,untype操作
    peopleDS.select($"name",$"age"+1).show()//使用表达式,scala的语法  要用$符号作为前缀
    peopleDS.filter($"age">21).show()//filter操作+表达式的一个应用
    peopleDS.groupBy("age").count().show()

    peopleDS.createOrReplaceTempView("people")
    val ageNotNullDS: DataFrame = spark.sql("select * from people where age is not null")
    ageNotNullDS.show()



    spark.stop()


  }

}
