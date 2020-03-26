package cn.spark.study.project

import cn.spark.study.project.utils.ContextUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

object SparkTest {


  def main(args: Array[String]): Unit = {
     val ssc: StreamingContext = ContextUtils.getStreamingContext(this.getClass.getSimpleName,5)



  }

}
