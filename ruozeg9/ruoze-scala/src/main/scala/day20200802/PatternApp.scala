package day20200802


import scala.util.Random

object PatternApp {

  def main(args: Array[String]): Unit = {
    val teachers = Array("Rola Takizawa", "Aoi Sala", "YuiHatano")

    val teacher = teachers(Random.nextInt(teachers.length))


    teacher match {
      case "Rola Takizawa" => println("泷老师")
      case "Aoi Sala" => println("苍老师")
      case _ => println("真不知道你们在说啥。。。。。")
    }


    class Person(val name: String, val age: Int)

    case object Person {
      def unapply(person: Person): Option[(String, Int)] = {
        if (null != person) {
          Some(person.name, person.age)
        } else {
          None
        }
      }
    }




    val p = new Person("PK", 30)

    p match {
      case Person(name, age) => println(name, age)
      case _ => println("other")
    }

  }

}
