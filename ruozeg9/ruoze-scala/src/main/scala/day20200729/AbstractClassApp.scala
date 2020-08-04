package day20200729

/**
 * 抽象类
 * 类的一个或多个方法没有完整实现，只有定义
 * 属性和抽象方法是不需要abstract修饰
 */
object AbstractClassApp {

  def main(args: Array[String]): Unit = {
   /* val student = new Student2

    student.speak*/

    println(Season(2))
  }

}




object Season extends Enumeration{
  val SPRING = Value(1,"spring")

  val SUMMER = Value(2,"summer")

  val AUTUMENTAL = Value(3,"autumental")

  val WINTER = Value(4,"winter")
}

abstract class Person2 {
  val name: String
  val age: Int

  def speak
}


class Student2 extends Person2 {
  override val name: String = "pk"
  override val age: Int = 31

  override def speak: Unit = {
    println(s"$name.......speaking......")
  }
}


case object T1
