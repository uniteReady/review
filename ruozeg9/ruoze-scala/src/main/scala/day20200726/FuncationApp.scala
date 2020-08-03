package day20200726

object FuncationApp {


  def sum(x:Int*): Int ={
    var result = 0;
    for (i <-x){
      result += i
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(1, 2, 3, 4, 5, 6)
    println(sum(arr:_*))
    println(sum(1 to 5: _*))
  }

}
