package com.tianya.bigdata.homework.day20200901.sparketl.etl

import com.tianya.bigdata.homework.day20200901.sparketl.utils.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProvinceSQL2RDDApp {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

//    sc.hadoopConfiguration.set("fs.defaultFS","hdfs://hadoop01:9000")
//    sc.hadoopConfiguration.set("dfs.replication","1")
//    System.setProperty("HADOOP_USER_NAME","hadoop")
    val dt = args(0)
    val inputPath = s"/ruozedata/dw/ods/access/d=$dt/part-r-00000"
    val outputPath = s"/ruozedata/dw/ods/spark_access/d=$dt"

    FileUtils.delete(sc.hadoopConfiguration,outputPath)

    val hdfsRDD = sc.textFile(inputPath)

    val provinceTupleRDD: RDD[(String, (Long, Int))] = hdfsRDD.map(log => {
      val splits = log.split("\t")
      val province = splits(12)
      val responseSize = splits(8).toLong
      (province, (responseSize, 1))
    })

    val provinceStatRDD: RDD[(String, (Long, Int))] = provinceTupleRDD.reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })

    provinceStatRDD.map(x => {x._1+"\t"+x._2._1+"\t"+x._2._2}).saveAsTextFile(outputPath)
    sc.stop()
  }

}
