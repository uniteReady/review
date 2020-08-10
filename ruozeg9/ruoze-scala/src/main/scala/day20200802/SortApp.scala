package day20200802

object SortApp {


  def main(args: Array[String]): Unit = {
    val list = List(20,50,30,10,5,500)

    //升序
    list.sortBy(x =>x)
    //降序
    list.sortBy(x => -x)
  }

}
