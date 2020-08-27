package com.tianya.bigdata.tututu.homework.tu20200824


import com.tianya.bigdata.homework.day20200812.LogParser
import com.tianya.bigdata.homework.day20200812.domain.Access
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object SparkETLJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//    conf.setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val textFilePath = args(0)
    val outPath = args(1)
//    val textFilePath = "out/access.log"
//    val textFilePath = "out/cuowu.txt"
//    val textFilePath = "out/cuowu2.txt"
    val fileRecordRDD: RDD[String] = sc.textFile(textFilePath)
    val filterRDDs = fileRecordRDD.filter(x => {
      !"-".equals(x.split("\t")(9))
    })



    val accessRDDs: RDD[Access] = filterRDDs.mapPartitions(
    x => {
        x.map(
          log => {
            try{
              val access = LogParser.parseLog3(log)
              access
            }catch {
              case e: NumberFormatException =>{
                null
              }
            }

          }
        )
      }
    )

    val filterRDD = accessRDDs.filter(null != _)
    filterRDD.cache()
    println("读进来的原始数据条数："+fileRecordRDD.count())
    println("经过过滤之后还剩下："+filterRDDs.count())
    println("经过mapPartition处理之后："+accessRDDs.count())
    println("过滤掉处理过程中报错的之后还剩下："+filterRDD.count())

    filterRDD.saveAsTextFile(outPath)


    /*fileRecordRDD.foreachPartition(

      x => {
        val searcher = new DbSearcher(new DbConfig(), dbPath)

        try {
          val accesses: Iterator[Access] = x.map(
            log => {
              val access = LogParser.parseLog(searcher, log)
              accessFormatsAccumulator.add(1L)
              access
            }
          )
          println(accesses.size)
          val accesses1: Iterator[Access] = accesses.take(5)
          for (elem <- accesses1) {
            println(elem)
          }
          accesses
        } catch {
          case e: Exception => {
            accessErrorAccumulator.add(1L)
          }
        }
      }

    )*/



    sc.stop()
  }

}
