package cn.spark.study.sql.typeOperation

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TypedOperation {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("TypedOperation").master("local").getOrCreate()
    import spark.implicits._
    val path = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\employees.json"
    val employeesDF: DataFrame = spark.read.json(path)
    val employeesDS: Dataset[Employee] = employeesDF.as[Employee]
    println(employeesDS.rdd.partitions.size)
//    employeesDS.show()
    /**
      * coalesce和repartition操作
      * 都是用来重新定义分区的
      * 区别在于:coalesce,只能用于减少分区数量,而且不会发生shuffle
      * repartition,可以增加分区数,也可以用于减少分区数,必须发生shuffle,相当于进行了一个重分区操作
      */
   /* val employeeDSRepartitioned: Dataset[Employee] = employeesDS.repartition(8)
    println(employeeDSRepartitioned.rdd.partitions.size)

    val employeeDSCoalesced: Dataset[Employee] = employeesDS.coalesce(4)

    println(employeeDSCoalesced.rdd.partitions.size)

    employeesDS.show()*/

    /**
      * distinct和dropDuplicates
      * 都是用来进行去重的,区别在哪儿呢
      * distinct,是根据每一条数据,进行完整内容的比对后去重
      * dropDuplicates,可以根据指定字段进行去重
      */

    /*val employeeDistinctedDS: Dataset[Employee] = employeesDS.distinct()
    employeeDistinctedDS.show()

    val employeeDropDuplicateDS: Dataset[Employee] = employeesDS.dropDuplicates("name")
    employeeDropDuplicateDS.show()*/

    /**
      * except filter intersect的区别:
      * except:获取在当前dataset中有,但是在另外一个dataset中没有的元素
      * filter:根据我们自己的逻辑 ,如果返回true,那么就保留该元素,否则就过滤掉该元素
      * intersect:获取两个数据集的交集
      */

    /*val path2 = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\employees2.json"
    val employeeDF2: DataFrame = spark.read.json(path2)
    val employeesDS2: Dataset[Employee] = employeeDF2.as[Employee]

    //except
    val employeesExceptDS: Dataset[Employee] = employeesDS.except(employeesDS2)
    employeesExceptDS.show()
    //intersect
    val employeesIntersectDS: Dataset[Employee] = employeesDS.intersect(employeesDS2)
    employeesIntersectDS.show()*/


    /**
      * joinWith
      */
    /*val path3 = "F:\\tianyafu\\tianyafu_github\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\department.json"
    val departmentDF: DataFrame = spark.read.json(path3)
    val departmentDS: Dataset[Department] = departmentDF.as[Department]
    val joinWithDS: Dataset[(Employee, Department)] = employeesDS.joinWith(departmentDS,$"name"===$"name1")
    joinWithDS.show()*/

    /**
      * sort
      */
    /*employeesDS.sort($"salary".desc).show()*/

    /**
      * randomSplit和sample的区别
      * randomSplit可以根据给定的权重值将数据集按照给定的分片数拆分开,Array中元素的个数就是分片数
      * sample:按照给定的百分比抽取数据
      *
      */
    employeesDS.randomSplit(Array(8,10,20)).foreach(_.show())
    employeesDS.sample(false,0.3).show()



    spark.stop()

  }

}

case class Employee(name: String, salary: Long)

case class Department(name1:String ,age : Long)
