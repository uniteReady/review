package com.tianya.bigdata.homework.day20200805

import scala.collection.mutable.ListBuffer

/**
 * 封装从Hive中查询出的流量数据
 *
 * @param province    省份
 * @param requestSize 请求大小
 */
case class ProvinceData(province: String, requestSize: Int)

/**
 * 封装结果
 *
 * @param province 省份
 * @param traffics 省份访问流量总和
 * @param cnt      省份请求次数
 */
case class Result(province: String, traffics: Int, cnt: Int)

object HiveSql2Scala {


  def main(args: Array[String]): Unit = {

    val results: ListBuffer[Result] = getResults()
    for (ele <- results) {
      println(ele)
    }

  }


  /**
   * 获取每个省的访问流量总和以及访问次数
   *
   * @return
   */
  def getResults(): ListBuffer[Result] = {
    //从Hive中获取数据
    val provinceDatas: ListBuffer[ProvinceData] = mockData()
    //转成List((province,ProvinceData))之后，做groupBy(province)
    val provinceDataMap: Map[String, ListBuffer[(String, ProvinceData)]] = provinceDatas.map(x => (x.province, x)).groupBy(_._1)
    //获取结果
    val results: ListBuffer[Result] = new ListBuffer[Result]
    provinceDataMap.toList.map(x => {
      val province: String = x._1
      val cnt: Int = x._2.size
      var sum: Int = 0
      x._2.map(y => {
        sum += y._2.requestSize
      })
      results.append(Result(province, sum, cnt))
    })
    results
  }

  /**
   * 从Hive中获取省份数据和对应的请求大小
   *
   * @return
   */
  def mockData(): ListBuffer[ProvinceData] = {
    val provinceDatas = new ListBuffer[ProvinceData]
    provinceDatas.append(ProvinceData("浙江", 20))
    provinceDatas.append(ProvinceData("浙江", 30))
    provinceDatas.append(ProvinceData("江苏", 20))
    provinceDatas.append(ProvinceData("上海", 40))
    provinceDatas.append(ProvinceData("上海", 50))
    provinceDatas
  }
}



