package com.tianya.bigdata.spark.day20200901

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkHBaseApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("1", "pk", 30), ("2", "ruoze", 31), ("3", "xingxing", 18)))

    val configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM,"hadoop01")
    configuration.set(TableOutputFormat.OUTPUT_TABLE,"user")

    val job: Job = Job.getInstance(configuration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.map(x => {
      val put = new Put(Bytes.toBytes(x._1))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("name"), Bytes.toBytes(x._2))
      put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("age"), Bytes.toBytes(x._3))
      (new ImmutableBytesWritable(), put)
    })

    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }

}
