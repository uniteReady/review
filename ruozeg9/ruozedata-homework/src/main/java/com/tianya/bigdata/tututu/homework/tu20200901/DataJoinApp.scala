package com.tianya.bigdata.tututu.homework.tu20200901

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 0901作业，要自定义的二次排序
 */
object DataJoinApp {
  def main(args: Array[String]): Unit = {
    val userPath = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200901/data/user.txt"
    val accessPath = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200901/data/access.txt"
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val userLinesRDD: RDD[String] = sc.textFile(userPath)
    val accessLinesRDD: RDD[String] = sc.textFile(accessPath)

    //变成 id,User的pair形式
    val userRDD: RDD[(String, User)] = userLinesRDD.map(line => {
      val splits = line.split(" ")
      (splits(0), User(splits(0).toInt, splits(1), splits(2)))
    })

    //变成 id,Access的pair形式
    val accessRDD = accessLinesRDD.map(line => {
      val splits = line.split(" ")
      (splits(0), Access(splits(0).toInt, splits(1).toInt, splits(2).toInt, splits(3).toInt))
    })

    val joinRDD: RDD[(String, (User, Option[Access]))] = userRDD.leftOuterJoin(accessRDD)
    val unSortRDD: RDD[(SecondarySortKey, UserAccessRecord)] = joinRDD.map(x => {
      val id: String = x._1
      val user: User = x._2._1
      val accessOption: Option[Access] = x._2._2
      //      val access = accessOption.getOrElse(Access(-1, -1, -1, -1))
      if (accessOption.isEmpty) {
        (new SecondarySortKey(id.toInt, -1, -1), UserAccessRecord(id.toInt, user.city, user.name, "null null null"))
      } else {
        val access = accessOption.getOrElse(Access(-1, -1, -1, -1))
        (new SecondarySortKey(id.toInt, access.year, access.month), UserAccessRecord(id.toInt, user.city, user.name, access.year + " " + access.month + " " + access.traffic))
      }
    })
    //根据自定义的二次排序对数据做排序
    val sortedRDD: RDD[UserAccessRecord] = unSortRDD.sortByKey().map(_._2)
    val pairRDD: RDD[(Int, UserAccessRecord)] = sortedRDD.map(record => (record.id, record))

    val resultPairRDD: RDD[(Int, UserAccessRecord)] = pairRDD.reduceByKey((x, y) => {
      UserAccessRecord(x.id, x.city, x.name, x.items + "|" + y.items)
    })

    val resultRDD: RDD[UserAccessRecord] = resultPairRDD.map(_._2)
    resultRDD.foreach(println)
    sc.stop()
  }

}


case class User(id: Int, city: String, name: String)

case class Access(id: Int, year: Int, month: Int, traffic: Int)

class SecondarySortKey(val id: Int, val year: Int, val month: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.id == that.id) {
      if (this.year - that.year != 0) {
        that.year - this.year
      } else {
        that.month - this.month
      }
    } else {
      0
    }
  }
}

case class UserAccessRecord(id: Int, city: String, name: String, items: String) {
  override def toString: String = id + "\t" + city + "\t" + name + "\t" + items
}