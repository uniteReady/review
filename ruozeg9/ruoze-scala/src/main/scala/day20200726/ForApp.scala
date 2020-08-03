package day20200726

object ForApp {

  def main(args: Array[String]): Unit = {
    val abc: String = "abc"
    for (ele <- abc) {
      println(ele+0)
    }
  }

}
