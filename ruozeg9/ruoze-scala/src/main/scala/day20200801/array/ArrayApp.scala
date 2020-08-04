package day20200801.array

import scala.collection.mutable.ArrayBuffer

object ArrayApp {

  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4)

//    println(arr(1))

    val arr1 = arr :+ 5
//    arr1.foreach(println)

    val arr2 = 100+:arr
//    arr2.foreach(println)

//    for (ele <- arr2){
//      println(ele)
//    }


//    println(arr1.mkString)



    val arrayBuffer = ArrayBuffer[Int]()
  }

}
