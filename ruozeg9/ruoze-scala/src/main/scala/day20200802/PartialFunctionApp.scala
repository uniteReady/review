package day20200802

import scala.util.Random

/**
 * 偏函数:被包在花括号里面的没有match的一组case语句
 *
 * 对符合条件的数据进行操作，即对部分数据进行操作的场景，用偏函数
 *
 * PartialFunction[-A, +B]
 *
 * A:代表的是输入的参数类型
 * B:代表的是输出的参数类型
 */
object PartialFunctionApp {
  def main(args: Array[String]): Unit = {
    val teachers = Array("Rola Takizawa", "Aoi Sala", "YuiHatano")

    val teacher = teachers(Random.nextInt(teachers.length))

    println(hello("Rola Takizawa"))


  }

  def hello: PartialFunction[String,String] ={
    case "Rola Takizawa" => "泷老师"
    case "Aoi Sala" => "苍老师"
    case _ => "真不知道你们在说啥。。。。。"
  }



  //需求，将 list中的数字*10 并输出
  val list =List(1,2,3,4,5,"你好", "hello")


  //偏函数写法中比较low的一种
  /*val part = new PartialFunction[Any,Int] {
    override def isDefinedAt(x: Any) = {
      if(x.isInstanceOf[Int]) true else false
    }

    override def apply(v1: Any) = v1.asInstanceOf[Int]* 10
  }


  list.collect(part).foreach(println)*/



  //这种方式也low
  /*def part:PartialFunction[Any,Int]={
    case i:Int =>i * 10
  }*/

  //这种方式还是low
  /*list.collect(
    {
      case i:Int =>i * 10
    }.foreach(println)
  )*/

  list.collect {
      case i:Int =>i * 10
    }.foreach(println)


}
