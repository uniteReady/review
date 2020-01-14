object Test {

  def main(args: Array[String]): Unit = {
    untilTest()
  }

  def untilTest(): Unit ={
    for(x <- 4 to 0 by -1){
      println(x)
    }
  }

}
