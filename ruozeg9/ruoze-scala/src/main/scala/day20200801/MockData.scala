package day20200801

import java.time.LocalDateTime
import java.util.Locale

object MockData {

  import java.time.format.DateTimeFormatter

  val dtf1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val dtf2: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:hh:mm:ss Z", Locale.US)


  def main(args: Array[String]): Unit = {
    /*val caseLog =
      """
        |[9/Jun/2015:01:58:09 +0800] 192.168.15.75 - 1542 "-" "GET http://www.aliyun.com/index.html" 200 191 2830 MISS "Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)" "text/html"
        |""".stripMargin
    val splits = caseLog.split(" ")

    splits.foreach(println)*/

    mockDateTime




  }


  def mockDateTime():String = {
    val now = LocalDateTime.now()

    val str = dtf1.format(now)
    val accessor = dtf1.parse(str)
    println(accessor)

    str
  }
}
