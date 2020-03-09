package cn.spark.study.project.spark

import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

object UserActiveDegreeAnalyze {

  def main(args: Array[String]): Unit = {

    // 如果是按照课程之前的模块，或者整套交互式分析系统的架构，应该先从mysql中提取用户指定的参数（java web系统提供界面供用户选择，然后java web系统将参数写入mysql中）// 如果是按照课程之前的模块，或者整套交互式分析系统的架构，应该先从mysql中提取用户指定的参数（java web系统提供界面供用户选择，然后java web系统将参数写入mysql中）

    // 但是这里已经讲了，之前的环境已经没有了，所以本次升级从简
    // 我们就直接定义一个日期范围，来模拟获取了参数
    val startDate = "2016-09-01"
    val endDate = "2016-11-01"

    val spark = SparkSession.builder().master("local").appName("UserActiveDegreeAnalyze").getOrCreate()

    import  spark.implicits._;

    val user_base_info_path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-bussiness-data-analysis-zhonghuashishan\\src\\main\\resources\\data\\user_base_info.json"
    val user_action_log_path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-bussiness-data-analysis-zhonghuashishan\\src\\main\\resources\\data\\user_action_log.json"

    val user_base_info_df = spark.read.json(user_base_info_path)
    val user_action_log_df = spark.read.json(user_action_log_path)

    // 第一个功能：统计指定时间范围内的访问次数最多的10个用户
    // 说明：课程，所以数据不会搞的太多，但是一般来说，pm产品经理，都会抽取100个~1000个用户，供他们仔细分析

    user_action_log_df
      .filter("actionTime >= '"+startDate+"' and actionTime <= '"+ endDate+"'")
        .join(user_base_info_df,"userId")
      // 第三部：进行分组，按照userid和username
        .groupBy(user_base_info_df("userId"), user_base_info_df("username")).agg(count(user_action_log_df("logId")))
        .show()


    spark.stop()

  }

}
