object HelloWorldApp {

  def main(args: Array[String]): Unit = {
    val x = 10
    val y = 11
    if(x >y) printf(s"$x 大于 $y") else printf(s"$x 小于 $y")
  }

}