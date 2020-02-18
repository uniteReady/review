package cn.spark.study.sql.untypeOperation

import cn.spark.study.sql.typeOperation.{Department, Employee}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * untyped操作:观察一下就会发现,实际上基本上就涵盖了普通sql语法的全部了
  * sql语法:
  * select
  * where
  * join
  * groupBy
  * agg
  */
object UnTypeOperation {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("UnTypeOperation").getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val employeePath = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\employees.json"
    val departmentPath = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\department2.json"
    val employeesDF: DataFrame = spark.read.json(employeePath)
    val departmentDF: DataFrame = spark.read.json(departmentPath)
    val employeesDS: Dataset[Employee] = employeesDF.as[Employee]
    val departmentDS: Dataset[Department2] = departmentDF.as[Department2]

    //where join groupBy agg
    employeesDS
      .where($"name"=!="james")
      .join(departmentDS,$"name"===$"name1")
      .groupBy(employeesDS("name"),departmentDS("gender"))
      .agg(avg(employeesDS("salary")))
      .show()
    //select
    employeesDS.select($"name").show()


    spark.stop()

  }

}

case class Department2(name1:String,gender:String)
