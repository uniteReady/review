package cn.spark.study.tuningSpark;

/**
 * spark性能优化
 */
public class TuningSpark {

    public static void main(String[] args) {
        /**
         * 性能优化
         *
         * 1.诊断内存消耗：将RDD手动指定partition的数量，然后调用persist方法将数据缓存到内存中，
         * 在Driver的log中可以看到单个partition占用了多少内存，然后乘以partition的个数就是大概的占用内存数量
         *
         * 2.使用高性能的序列化类库：使用kryo序列化类库替代默认的java序列化
         *
         * 3.优化数据结构：用String来替代Map  用array来替代一些ArrayList、LinkedList等
         *
         * 4.对多次使用的RDD提前进行persist或者checkpoint操作,如果使用persist的话，可以使用带序列化的持久化方式，
         * 这样序列化之后的数据占用空间小，即使需要磁盘IO ，IO也会较小
         *
         * 5.优化内存使得减少GC发生的频率，调整eden区域的大小或者调整幸存区的比例，或者调整老年代和新生代的比例
         *
         * 6.提高并行度，官方建议并行度是给每个CPU的核设置2到3个task：http://spark.apache.org/docs/2.3.3/tuning.html#level-of-parallelism
         *
         * 7.使用广播变量来广播大变量，这样就使得每个节点上保留一份大变量的副本，而不是每个task保留一份： http://spark.apache.org/docs/2.3.3/tuning.html#broadcasting-large-variables
         *
         * 8.调整spark locality系列参数，使得尽量使用最高的本地化级别来调度Task  http://spark.apache.org/docs/2.3.3/configuration.html#scheduling
         *
         *
         * 9.通过调参数来提高shuffle的性能
         */

    }
}


