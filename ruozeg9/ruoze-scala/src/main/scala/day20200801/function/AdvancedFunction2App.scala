package day20200801.function

object AdvancedFunction2App {


  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3, 4, 5)

    def my_map(array: Array[Int], op: Int => Int) = {
      for (ele <- array) yield op(ele)
    }

//    my_map(arr, _ * 2).foreach(println)


    def my_filter(array: Array[Int], op: Int => Boolean) = {
      for (ele <- array if op(ele))  yield ele
    }

//    my_filter(arr,_ > 3).foreach(println)

    def my_foreach(arr: Array[Int], op: Int =>Unit)={
      for(ele <- arr) yield op(ele)
    }

    my_foreach(arr,x => println(x))



    def my_reduce(arr: Array[Int], op: (Int, Int) =>Int):Int = {
      var last = arr(0)
      for ( i <- 1 until arr.length) {
        last = op(last,arr(i))
      }
      last
    }

    println(my_reduce(arr, _ + _))

  }

}
