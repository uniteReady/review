package day20200801.tuple

object TupleApp {

  def main(args: Array[String]): Unit = {
    val a = (1,2,3,4,5)

    //遍历tuple
    for ( i <- 0 until a.productArity){
      println(a.productElement(i))
    }

    for (ele <- a.productIterator){
      println(ele)
    }

    //对偶交换
    val b = ("左边","右边")
    val c = b.swap
    println(c)
  }

}
