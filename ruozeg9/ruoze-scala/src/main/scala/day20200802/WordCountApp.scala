package day20200802

import scala.io.Source

object WordCountApp {


  def main(args: Array[String]): Unit = {
    val lines: List[String] = Source.fromFile("ruozeg9/ruoze-scala/src/main/scala/day20200802/input.txt").getLines().toList

    /* val wordCounts : Map[String,List[String]] =lines.flatMap(_.split(",")).map(_.trim.toLowerCase).groupBy(x =>x)
     wordCounts.mapValues(x =>x.size)*/

    lines
      .flatMap(_.split(","))
      .map(x => (x.trim.toLowerCase,1))
      .groupBy(_._1)
      .mapValues(_.size)
      .toList
      .sortBy(-_._2)
      .take(2).foreach(println)


    



  }

}
