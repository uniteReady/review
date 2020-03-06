package cn.spark.study.project.spark.ad;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.IAdUserClickCountDAO;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.AdUserClickCount;
import cn.spark.study.project.utils.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计spark作业
 * @author Administrator
 *
 */
public class AdClickRealTimeStatSpark {

    public static void main(String[] args) throws Exception {
        //创建spark streaming的上下文
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //创建一份kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_KEY_DESERIALIZER));
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_VALUE_DESERIALIZER));
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID));
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_AUTO_OFFSET_RESET));
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConfigurationManager.getBoolean(Constants.KAFKA_AUTO_COMMIT));
        //配置kafka topic
        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPIC);
        String[] splits = topics.split(",");
        List<String> topicList = Arrays.asList(splits);

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream<val1,val2>
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicList, kafkaParams));

        //日志格式：timestamp province city userid adid
        // 某个时间点 某个省份 某个城市 某个用户 某个广告

        //计算出每5秒一个batch中 每天每个用户每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(ConsumerRecord<String, String> record) throws Exception {
                String log = record.value();
                String[] splits = log.split(" ");
                String timestamp = splits[0];
                Date date = new Date(Long.valueOf(timestamp));
                String dateKey = DateUtils.formatDateKey(date);
                String userid = splits[3];
                String adid = splits[4];
                String key = dateKey + "_" + userid + "_" + adid;
                return new Tuple2<>(key, 1L);
            }
        });
        //针对处理后的日志格式，执行reduceByKey算子 得到每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        //到这里为止，获取到了什么数据呢？我们得到了每天每个用户对每只广告的点击次数
        //<yyyyMMdd_userid_adid,count>
        dailyUserAdClickCountDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> pairRDD) throws Exception {
                /**
                 * 正确高效的使用spark streaming 往mysql中插入数据
                 * 每个RDD的每个partition去获取一次连接
                 */
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdUserClickCount> adUserClickCounts = new ArrayList<>();
                        while (iterator.hasNext()){
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] splits = tuple._1.split("_");
                            String date = splits[0];
                            //变成yyyy-MM-dd
                            date = DateUtils.formatDate(DateUtils.parseDateKey(date));
                            Long userid = Long.valueOf(splits[1]);
                            Long adid = Long.valueOf(splits[2]);
                            Long count = tuple._2;
                            adUserClickCounts.add(new AdUserClickCount(date,userid,adid,count));
                        }
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        adUserClickCountDAO.updateBatch(adUserClickCounts);

                    }
                });

            }
        });

        //启动spark streaming并等待任务执行结束并关闭
        jssc.start();
        jssc.awaitTermination();

        jssc.stop();
    }
}
