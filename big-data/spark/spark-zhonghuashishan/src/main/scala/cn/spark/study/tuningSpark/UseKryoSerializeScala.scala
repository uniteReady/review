package cn.spark.study.tuningSpark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 当算子函数使用到了外部的大数据的情况下，比如我们在外部顶一个了一个封装了应用所有配置的对象，比如自定义了一个MyConfiguration对象
  * 里面包含了100MB的数据，然后，在算子函数里面，使用到了这个外部的大对象，此时，如果使用java序列化机制的话，不仅序列化速度缓慢，
  * 并且序列化之后的数据还是比较大，比较占用内存空间。因此，这种情况下，比较适合使用kryo进行序列化，一是序列化速度快
  * ，二是会减少内存占用空间
  */
object UseKryoSerializeScala {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("UseKryoSerializeScala").setMaster("local")
    //设置为使用kryo进行序列化
    // 参见 http://spark.apache.org/docs/2.3.3/tuning.html#data-serialization
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //这个参数是每个worker节点上的每个核都会有这么一个序列化buffer，单位为KB，默认为64KB
    //如果你要序列化的对象很大，那么序列化之后的对象也可能很大，此时需要调整这个序列化buffer，否则可能kryo内部的buffer放不下
    // 参见 http://spark.apache.org/docs/2.3.3/configuration.html#compression-and-serialization
    conf.set("spark.kryoserializer.buffer", "2048")
    //允许使用的kryo序列化buffer的最大值，单位为MB，默认为64MB，最大不超过2048MB
    conf.set("spark.kryoserializer.buffer.max", "2047")
    //这里将需要使用Kryo的类进行注册,如果不注册，其实kryo也可以正常工作，
    // 但是kryo需要保留每个需要序列化的对象的全限定名，这样反而会比较浪费内存
    conf.registerKryoClasses(Array(classOf[A], classOf[B]))

    val sc = new SparkContext(conf)


    sc.stop()
  }

}

class A {}

class B {}
