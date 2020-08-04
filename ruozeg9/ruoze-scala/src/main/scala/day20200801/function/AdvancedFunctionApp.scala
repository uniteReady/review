package day20200801.function

object AdvancedFunctionApp {


  def main(args: Array[String]): Unit = {
    //定义方式一： val/var 函数名称 = (输入参数列表) => {函数体}
    val fun1 = (a: Int, b: Int) => a + b

    println(fun1(1, 3))

    //定义方式二：val/var 函数名称:(输入参数类型) => 返回值类型 = (输入参数的引用) => {函数体}
    val fun2:(Int,Int) => Int = (a,b)=>a + b
    println(fun2(2, 4))


    def sayHello(name:String): Unit ={
      println(s"Hello,$name")
    }
    //将方法赋值给一个变量
    val sayHello2 = sayHello _



    //高阶函数
    def add(a:Int,b:Int)=a + b

    def foo(op:(Int,Int)=>Int):Int ={
      op(10,20)
    }

    println(foo(add))



  }

}
