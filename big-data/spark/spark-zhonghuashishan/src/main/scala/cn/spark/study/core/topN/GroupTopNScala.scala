package cn.spark.study.core.topN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

object GroupTopNScala {

  def main(args: Array[String]): Unit = {
    val topN: Int = 4
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("GroupTopNScala")
    val sc = new SparkContext(conf)
    val path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\score.txt"
    val lineRDD: RDD[String] = sc.textFile(path)

    val pairRDD = lineRDD.map(x => {
      val lineSplited: Array[String] = x.split(" ")
      (lineSplited(0), lineSplited(1).toInt)
    })

    val groupedRDD: RDD[(String, Iterable[Int])] = pairRDD.groupByKey()
    val resultRDD = groupedRDD.map(
      x => {
        val className: String = x._1
        val scoresIterable: Iterable[Int] = x._2
        val it: Iterator[Int] = scoresIterable.iterator
        val topNArray = new Array[Integer](topN);

        while (it.hasNext) {
          val score: Int = it.next()
          breakable{
            for (i <- (0 until topNArray.length)) {
              if(topNArray(i) == null){
                topNArray(i)=score
                break()
              }else if( score >= topNArray(i)){
                for(j <- topNArray.length -1 until  i by -1){
                  topNArray(j) = topNArray(j-1)
                }
                topNArray(i)= score
                break()
              }
            }
        }
        }
        new Tuple2(className,topNArray)
      }
    )

    resultRDD.foreach(
      x =>{
        println("className : "+ x._1)
        x._2.foreach(println)
        println("=================")
      }
    )

    sc.stop()
  }

}
