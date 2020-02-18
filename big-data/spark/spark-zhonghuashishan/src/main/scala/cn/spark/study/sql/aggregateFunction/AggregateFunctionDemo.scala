package cn.spark.study.sql.aggregateFunction

import cn.spark.study.sql.typeOperation.Employee
import cn.spark.study.sql.untypeOperation.Department2
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 其他常用函数参考
  * http://spark.apache.org/docs/2.3.3/api/scala/index.html#org.apache.spark.sql.functions$
  */
object AggregateFunctionDemo {

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

   /* employeesDS
      .join(departmentDS,$"name"===$"name1")
      .groupBy(departmentDS("name1"))
      .agg(
        avg(employeesDS("salary")),
        sum(employeesDS("salary")),
        min(employeesDS("salary")),
        max(employeesDS("salary")),
        count($"gender")
        ,countDistinct(employeesDS("name"))
      ).show()*/

    /**
      * collect_list和collect_set,都用于将同一个分组内的指定字段的值串起来,变成一个数组
      * collect_set是去重的 collect_list是不去重的
      * 常用于行转列
      * 比如说:
      * deptId=1,employee=leo
      * deptId=1,employee=jack
      * 转换之后为:deptId=1,employees=[leo,jack]
      */
    employeesDS
      .groupBy(employeesDS("name"))
      .agg(collect_list(employeesDS("salary")),collect_set(employeesDS("salary")))
      .show()


    spark.stop()
  }

}
