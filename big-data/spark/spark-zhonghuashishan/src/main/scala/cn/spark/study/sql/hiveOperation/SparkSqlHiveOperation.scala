package cn.spark.study.sql.hiveOperation

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSqlHiveOperation {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SparkSqlHiveOperation").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val hdfsMaster: String = args(0)
    val hdfsPort: String = args(1)
    val hdfsPath = "hdfs://"+hdfsMaster+":"+hdfsPort+"/tmp/tianyafu/kv1.txt"
    println("this is hdfsPath : "+ hdfsPath)


    spark.sql("create table if not exists src(key int ,value string)")
    spark.sql("load data inpath '"+hdfsPath+"' into table src")
   spark.sql("select * from src").show()
    spark.sql("select count(*) from src").show()
    val sqlDF: DataFrame = spark.sql("select key,value from src where key <10 order by key")

    val sqlHiveDS: Dataset[String] = sqlDF.map({case Row(key:Int,value:String) =>s"KEY:$key,VALUE:$value"})
    sqlHiveDS.show()

    spark.stop()
  }

}


case class Src(key:Int ,value : String)
