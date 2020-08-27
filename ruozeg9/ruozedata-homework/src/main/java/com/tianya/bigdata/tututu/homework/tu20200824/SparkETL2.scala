package com.tianya.bigdata.tututu.homework.tu20200824

import java.lang.reflect.Method
import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.tianya.bigdata.homework.day20200812.domain.Access
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.{SparkConf, SparkContext}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object SparkETL2 {

  def main(args: Array[String]): Unit = {
    val dbPath = "ip2region.db"
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[DbConfig], classOf[DbSearcher], classOf[Method]))
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

    //      .setMaster("local[4]").setAppName(this.getClass.getName)


    // 标记为lazy的 等到用的时候才去读取hdfs上的ip2region.db文件
    lazy val config: DbConfig = SparkLogETLUtils.getDbConfig
    lazy val searcher: DbSearcher = SparkLogETLUtils.getDbSearcher(config, dbPath)
    lazy val m: Method = SparkLogETLUtils.getMethod(searcher)

    //先过滤脏数据，filter之后再处理日志解析。这里因为本身只有904条脏数据，filter之后就不用coalesce算子了。
    sc.textFile(input).filter(x => {
      !"-".equals(x.split("\t")(9))
    }).map(log => {
      val access = SparkLogETLUtils.parseLog(log, searcher, m)
      access.toString
    }).saveAsTextFile(output)

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
