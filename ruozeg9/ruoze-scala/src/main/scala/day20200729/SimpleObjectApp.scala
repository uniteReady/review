package day20200729

import scala.beans.BeanProperty

object SimpleObjectApp {

  def main(args: Array[String]): Unit = {
    val user = new User()
    user.setName("张三")
    user.name_$eq("李四")
    println(user.name)
    println(user.age)
//    println(user.sex)
  }

}


class User{
  @BeanProperty
  var name = "pk"
  val age = 20

  private val sex = "男"


  def eat(): Unit ={
    println(s"${name} .......... eating.....")
  }
}
