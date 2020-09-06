package com.tianya.bigdata.spark.day20200901

import com.tianya.bigdata.spark.utils.FileUtils
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{SparkConf, SparkContext}

object DataSourceApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS","hdfs://hadoop01:9000")
    sc.hadoopConfiguration.set("dfs.replication","1")
    System.setProperty("HADOOP_USER_NAME","hadoop")
    val rdd = sc.parallelize(List(("pk",30),("ruoze",31),("xingxing",18)))

    val path = "hdfs://hadoop01:9000/ruozedata/spark/test"

    FileUtils.delete(sc.hadoopConfiguration,path)

//    rdd.saveAsTextFile(path)
    rdd.saveAsTextFile(path,classOf[BZip2Codec])






    sc.stop()

  }



}
