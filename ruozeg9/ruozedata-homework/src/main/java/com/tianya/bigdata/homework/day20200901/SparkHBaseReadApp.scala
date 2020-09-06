package com.tianya.bigdata.homework.day20200901

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Base64
object SparkHBaseReadApp {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "hadoop01")
    configuration.set(TableInputFormat.INPUT_TABLE, "user")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val hbaseDataRDD: RDD[(String, String, Int)] = hbaseRDD.map(x => {
      val result: Result = x._2
      val rowKey = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(Bytes.toBytes("o"), Bytes.toBytes("name")))
      val age = Bytes.toInt(result.getValue(Bytes.toBytes("o"), Bytes.toBytes("age")))
      (rowKey, name, age)
    })

    hbaseDataRDD.foreach(println)
    sc.stop()
  }
}