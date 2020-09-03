package com.tianya.bigdata.tututu.homework.tu20200901

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataJoinApp {


  /**
   * 假设access.txt是小表，access.txt是大表,用broadcast把access.txt给广播出去
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val userPath = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200901/data/user.txt"
    val accessPath = "ruozeg9/ruozedata-homework/src/main/java/com/tianya/bigdata/tututu/homework/tu20200901/data/access.txt"
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)

    val userLinesRDD: RDD[String] = sc.textFile(userPath)
    val accessLinesRDD: RDD[String] = sc.textFile(accessPath)

    val userRDD: RDD[(String, User)] = userLinesRDD.map(line => {
      val splits = line.split(" ")
      (splits(0), User(splits(0).toInt, splits(1), splits(2)))
    })

    val accessRDD = accessLinesRDD.map(line => {
      val splits = line.split(" ")
      (splits(0), Access(splits(0).toInt, splits(1).toInt, splits(2).toInt, splits(3).toInt))
    })

    val joinRDD: RDD[(String, (User, Option[Access]))] = userRDD.leftOuterJoin(accessRDD)

    //    joinRDD.foreach(println)
    val unSortRDD: RDD[(SecondarySortKey, UserAccessRecord)] = joinRDD.map(x => {
      val id: String = x._1
      val user: User = x._2._1
      val accessOption: Option[Access] = x._2._2
//      val access = accessOption.getOrElse(Access(-1, -1, -1, -1))
      if(accessOption.isEmpty) {
        (new SecondarySortKey(id.toInt, -1, -1), UserAccessRecord(id.toInt, user.city, user.name, "null null null"))
      }else{
        val access = accessOption.getOrElse(Access(-1, -1, -1, -1))
        (new SecondarySortKey(id.toInt, access.year, access.month), UserAccessRecord(id.toInt, user.city, user.name, access.year+" "+access.month+" "+access.traffic))
      }
    })

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

case class UserAccessRecord(val id: Int
                            , val city: String
                            , val name: String
                            , val items: String)