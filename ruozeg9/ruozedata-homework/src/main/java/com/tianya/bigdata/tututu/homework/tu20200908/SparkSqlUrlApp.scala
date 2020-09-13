package com.tianya.bigdata.tututu.homework.tu20200908

import org.apache.spark.sql.SparkSession

object SparkSqlUrlApp {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()


    val sql =
      """
        |with t as
        |    (select
        |    url,
        |    count(url) over(partition by url ) cnt,
        |    count(url) over() total
        |    from ruozedata.ods_access)
        |    select t.url,t.cnt,t.total,round(t.cnt*100/t.total,2) as ratio from t
        |    group by t.url,t.cnt,t.total order by t.cnt desc limit 20
        |""".stripMargin

    spark.sql(sql).show()





    spark.stop()
  }

}
