package com.tianya.bigdata.tututu.homework.tu20200824

import java.lang.reflect.Method
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.tianya.bigdata.homework.day20200812.domain.Access
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

/**
 * 不需要用
 */
object SparkETL3 {

  def main(args: Array[String]): Unit = {
    //    val uri = "hdfs://hadoop01:9000"
    val dbPath = "ip2region.db"
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val executionTime: String = conf.get("spark.execute.time", "20190101")
    val input = s"/ruozedata/dw/raw/access/${executionTime}"
    val tmpPath = s"/ruozedata/dw/ods_tmp/access/${executionTime}"
    val output = s"/ruozedata/dw/ods/access/d=${executionTime}"
    val configuration = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(configuration)
    val tmpHDSFPath = new Path(tmpPath)
    if (fileSystem.exists(tmpHDSFPath)) {
      fileSystem.delete(tmpHDSFPath, true)
    }

    val m1: (DbSearcher) => Method = SparkLogETLUtils2.getMethod _

    val s1: (DbConfig, String) => DbSearcher = SparkLogETLUtils2.getDbSearcher _

    val c1: () => DbConfig = SparkLogETLUtils2.getDbConfig _

    val l1: (String, DbSearcher, Method) => Access = SparkLogETLUtils2.parseLog _


    val value: RDD[String] = sc.textFile(input).filter(x => {
      !"-".equals(x.split("\t")(9))
    }).mapPartitions(
      iter => {
        val searcher: DbSearcher = s1(c1(), dbPath)
        val method: Method = m1(s1(c1(), dbPath))
        iter.map(
          log => {
            val access = l1(log, searcher, method)
            access.toString
          }
        )
      }
    )
    value.saveAsTextFile(output)

    //移除对应的原有的分区目录
    println("开始移除对应的原有的分区目录" + output)
    val outputPath = new Path(output)
    if (fileSystem.exists(outputPath)) fileSystem.delete(outputPath, true)
    //创建对应的分区目录
    println("开始创建对应的分区目录" + output)
    fileSystem.mkdirs(outputPath)
    //从临时目录中移动数据到对应的分区目录下
    println("开始从临时目录" + tmpPath + "中移动数据到对应的分区目录下" + output)
    val remoteIterator: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(tmpHDSFPath, true)
    while (remoteIterator.hasNext) {
      val fileStatus = remoteIterator.next()
      val filePath = fileStatus.getPath()
      fileSystem.rename(filePath, outputPath)
    }

    //删除临时目录及数据
    println("开始删除临时目录及数据" + tmpPath)
    fileSystem.delete(tmpHDSFPath, true)

    sc.stop()
  }

}
