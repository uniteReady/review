package day20200729

object ConstructExtendsApp {

  def main(args: Array[String]): Unit = {
    /*val person = new Person("zhangsan", 30)
    println(person.name)

    val person1 = new Person("lisi", 31, "男")
    println(person1.gender)*/

    val student = new Student("wangwu", 18, "cs")
    student.gender = "女"

    println(student)
  }

}


class Person(val name: String,val age: Int){

  println("-----------------Person Constructor enter--------------")

  val school = "pku"
  var gender:String = _

  /**
   * 附属构造器，第一行要么调用主构造器，要么调用别的附属构造器
   * @param name
   * @param age
   * @param gender
   */
  def this(name:String,age:Int,gender:String){
    this(name,age)
    this.gender = gender
  }
  println("这是Person的构造器")

  println("-----------------Person Constructor leave--------------")
}

class Student(name: String,age: Int,val major:String) extends Person(name,age){

  println("------------Student Constructor enter---------------")

  //重写父类的属性
  override val school = "nudt"

  //重写toString
  override def toString: String = s"$name,$age,$major，$gender,$school"

  println("------------Student Constructor leave---------------")

}