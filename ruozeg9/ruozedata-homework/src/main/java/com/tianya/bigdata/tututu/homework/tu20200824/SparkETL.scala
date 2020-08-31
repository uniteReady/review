package com.tianya.bigdata.tututu.homework.tu20200824

import java.lang.reflect.Method

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.{SparkConf, SparkContext}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * ip2region.db
 * 该db库可以通过--jars传递进去，路径可以是本地的，也可以是hdfs的路径
 * 如果是hdfs的路径，对应的变量要用lazy修饰，否则会报文件找不到
 *
 * 这里主要会碰到第三方类没有序列化 导致该类的对象在driver中生成之后，无法在executor端使用
 * 有以下几种方式：
 * 1.使用@transient修饰该变量，使其不序列化 见 SparkETL
 * 2.使用kryo注册该类，在driver端传输到executor端时使用kryo来序列化 见 SparkETL2
 * 3.使用函数来替代序列化(不知道这么说对不对  不过函数是一等公民 实测可行) 见 SparkETL3
 */
object SparkETL {

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
    //使用 @transient 来避免序列化
    @transient lazy val config: DbConfig = SparkLogETLUtils.getDbConfig
    @transient lazy val searcher: DbSearcher = SparkLogETLUtils.getDbSearcher(config, dbPath)
    @transient lazy val m: Method = SparkLogETLUtils.getMethod(searcher)

    //先过滤脏数据，filter之后再处理日志解析。这里因为本身只有904条脏数据，filter之后就不用coalesce算子了。
    sc.textFile(input).filter(x => {
      !"-".equals(x.split("\t")(9))
    }).map(log => {
      val access = SparkLogETLUtils.parseLog(log, searcher, m)
      access.toString
    }).saveAsTextFile(tmpPath)

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
