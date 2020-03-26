package cn.spark.study.project.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ContextUtils {



  /**
    * 获取SparkContext
    * val sc: SparkContext = ContextUtils.getSparkContext(this.getClass.getSimpleName)
    *
    * @param appName appName
    * @param master  master
    * @return SparkContext
    */
  def getSparkContext(appName: String, master: String = "local[2]"): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    new SparkContext(conf)
  }

  /**
    * 获取StreamingContext
    * val ssc: StreamingContext = ContextUtils.getStreamingContext(this.getClass.getSimpleName,5)
    * @param appName appName
    * @param batch   batchInterval
    * @param master  master
    * @return StreamingContext
    */
  def getStreamingContext(appName: String, batch: Int, master: String = "local[4]"): StreamingContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    new StreamingContext(conf, Durations.seconds(batch))
  }

  /**
    * 获取SparkSession
    * val spark = ContextUtils.getSparkSession(this.getClass.getSimpleName,false)
    * @param appName           appName
    * @param enableHiveSupport 是否开启hive支持
    * @param master            master
    * @return SparkSession
    */
  def getSparkSession(appName: String, enableHiveSupport: Boolean, master: String = "local[2]"): SparkSession = {
    val builder: SparkSession.Builder = SparkSession.builder().appName(appName).master(master)
    if (enableHiveSupport) {
      builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }
}
