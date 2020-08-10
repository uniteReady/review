package day20200802

object SomeFunctionApp {


  def main(args: Array[String]): Unit = {
    val array = Array(1,2,3,4,5,6,7,8)


    println(array.find(x => x > 5))



    val list = List(1,2,3,4)

    val list2 = List('A','B','C','D')
    val list3 = List('A','B','C')

    println(list.zip(list2))
    println(list.zip(list3))

    println(list.zipAll(list3, "-", "~"))

    println(list.zipWithIndex)

    //groupBy
    val l = List(1,2,3,4,5,6,7,8)
    val groupBy: Map[String, List[Int]] = l.groupBy(x => if (x % 2 == 0) "偶数" else "奇数")

    groupBy.foreach(println)

    val arr = Array(('a',100),('b',200),('a',150),('c',300),('a',300),('b',400))

    val charToTuples: Map[Char, Array[(Char, Int)]] = arr.groupBy(x => x._1)
    val charToSum: Map[Char, Int] = charToTuples.mapValues(x => x.map(y => y._2).sum)





  }

}
