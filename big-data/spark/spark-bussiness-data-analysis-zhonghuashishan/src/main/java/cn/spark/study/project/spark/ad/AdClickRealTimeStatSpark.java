package cn.spark.study.project.spark.ad;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.*;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.*;
import cn.spark.study.project.utils.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.spark.api.java.Optional;

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
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //设置checkpoint目录
        jssc.checkpoint("hdfs://master:9000/tmp/tianyafu/checkpoint");

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

        // 根据动态黑名单进行数据过滤
        JavaDStream<ConsumerRecord<String, String>> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

        //生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        // 最粗
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

        // 业务功能二：实时统计每天每个省份top3热门广告
        // 统计的稍微细一些了
        calculateProvinceTop3Ad(spark,adRealTimeStatDStream);


        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        // 统计的非常细了
        // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
        // 每支广告的点击趋势
        calculateAdClickCountByWindow(adRealTimeLogDStream);

        //启动spark streaming并等待任务执行结束并关闭
        jssc.start();
        jssc.awaitTermination();

        jssc.stop();
    }





    /**
     * 计算最近1小时滑动窗口内的广告点击趋势
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream){
        // 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(ConsumerRecord<String, String> record) throws Exception {
                //日志格式：timestamp province city userid adid
                String log = record.value();
                String[] splits = log.split(" ");
                String dateHourMinute = DateUtils.formatTimeMinute(DateUtils.parseTime(splits[0]));
                String adId = splits[4];

                return new Tuple2<>(dateHourMinute + "_" + adId, 1L);
            }
        });

        // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
        // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
        // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
        // 1小时滑动窗口内的广告点击趋势
        // 点图 / 折线图
        JavaPairDStream<String, Long> aggredDStream = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10));

        // aggrRDD
        // 每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
        // 各广告，在最近1小时内，各分钟的点击量
        aggredDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> pairRDD) throws Exception {
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            // yyyyMMddHHmm
                            String dateMinute = keySplited[0];
                            long adid = Long.valueOf(keySplited[1]);
                            long clickCount = tuple._2;

                            String date = DateUtils.formatDate(DateUtils.parseDateKey(
                                    dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdid(adid);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrends.add(adClickTrend);
                        }

                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                        adClickTrendDAO.updateBatch(adClickTrends);
                    }
                });
            }
        });

    }

    /**
     * 计算每天各省份的top3热门广告
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(SparkSession spark,JavaPairDStream<String, Long> adRealTimeStatDStream){

        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> pairRDD) throws Exception {
                JavaPairRDD<String, Long> mapedRDD = pairRDD.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                        String[] splits = tuple._1.split("_");
                        Long clickCount = tuple._2;
                        String date = splits[0];
                        String province = splits[1];
                        String adId = splits[3];
                        String key = date + "_" + province + "_" + adId;
                        return new Tuple2<>(key, clickCount);
                    }
                });
                JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mapedRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

                // 将dailyAdClickCountByProvinceRDD转换为DataFrame
                // 注册为一张临时表
                // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告
                JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                        String[] splits = tuple._1.split("_");
                        Long clickCount = tuple._2;
                        String date = DateUtils.formatDate(DateUtils.parseDateKey(splits[0]));
                        String province = splits[1];
                        Long adId = Long.valueOf(splits[2]);
                        return RowFactory.create(date, province, adId, clickCount);
                    }
                });
                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("province", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("ad_id", DataTypes.LongType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.LongType, true));
                StructType structType = DataTypes.createStructType(structFields);

                Dataset<Row> rowDS = spark.createDataFrame(rowsRDD, structType);
                rowDS.createTempView("tmp_daily_ad_click_count_by_prov");

                String sql = " select " +
                        "date, " +
                        "province," +
                        "ad_id," +
                        "click_count " +
                        "from " +
                        "(select " +
                        "date, " +
                        "province," +
                        "ad_id," +
                        "click_count," +
                        "row_number() over(partition by province order by click_count desc) rank " +
                        "from tmp_daily_ad_click_count_by_prov" +
                        ") t " +
                        "where t.rank <=3 ";

                Dataset<Row> provinceTop3AdDF = spark.sql(sql);


                return provinceTop3AdDF.javaRDD();
            }
        });
        // rowsDStream
        // 每次都是刷新出来各个省份最热门的top3广告
        // 将其中的数据批量更新到MySQL中
        rowsDStream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {
                rowJavaRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<>();
                        while (iterator.hasNext()){
                            Row row = iterator.next();
                            String date = row.getString(0);
                            String province = row.getString(1);
                            long adId = row.getLong(2);
                            long clickCount = row.getLong(3);
                            adProvinceTop3s.add(new AdProvinceTop3(date,province,adId,clickCount));
                        }
                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }
                });
            }
        });

    }


    /**
     * 计算广告点击流量实时统计
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    public static JavaPairDStream<String, Long>  calculateRealTimeStat (JavaDStream<ConsumerRecord<String, String>> filteredAdRealTimeLogDStream){
        // 业务逻辑一
        // 广告点击流量实时统计
        // 上面的黑名单实际上是广告类的实时系统中，比较常见的一种基础的应用
        // 实际上，我们要实现的业务功能，不是黑名单

        // 计算每天各省各城市各广告的点击量
        // 这份数据，实时不断地更新到mysql中的，J2EE系统，是提供实时报表给用户查看的
        // j2ee系统每隔几秒钟，就从mysql中搂一次最新数据，每次都可能不一样
        // 设计出来几个维度：日期、省份、城市、广告
        // j2ee系统就可以非常的灵活
        // 用户可以看到，实时的数据，比如2015-11-01，历史数据
        // 2015-12-01，当天，可以看到当天所有的实时数据（动态改变），比如江苏省南京市
        // 广告可以进行选择（广告主、广告名称、广告类型来筛选一个出来）
        // 拿着date、province、city、adid，去mysql中查询最新的数据
        // 等等，基于这几个维度，以及这份动态改变的数据，是可以实现比较灵活的广告点击流量查看的功能的

        // date province city userid adid
        // date_province_city_adid，作为key；1作为value
        // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
        // 我们要对原始数据进行map，映射成<date_province_city_adid,1>格式
        // 然后呢，对上述格式的数据，执行updateStateByKey算子
        // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态

        //映射成JavaPairDStream<date_province_city_adid, 1L> 这种格式
        JavaPairDStream<String, Long> mapedDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(ConsumerRecord<String, String> record) throws Exception {
                String log = record.value();
                String[] splits = log.split(" ");
                String date = splits[0];
                //变成yyyy-MM-dd
                date = DateUtils.formatDate(DateUtils.parseDateKey(date));
                String province = splits[1];
                String city = splits[2];
                String adid = splits[4];
                String key = date + "_" + province + "_" + city + "" + adid;
                return new Tuple2<>(key, 1L);
            }
        });

        // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
        // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
        JavaPairDStream<String, Long> aggregatedDStream = mapedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            @Override
            public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {

                // 举例来说
                // 对于每个key，都会调用一次这个方法
                // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
                // 10个

                // values，(1,1,1,1,1,1,1,1,1,1)

                // 首先根据optional判断，之前这个key，是否有对应的状态

                Long clickCount = 0L;
                // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
                if(optional.isPresent()){
                    clickCount = optional.get();
                }
                // values，代表了，batch rdd中，每个key对应的所有的值
                for (Long value : values) {
                    clickCount += value;
                }
                return optional.of(clickCount);
            }
        });

        aggregatedDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdStat> adStats = new ArrayList<>();
                        while (iterator.hasNext()){
                            Tuple2<String, Long> tuple = iterator.next();
                            Long clickCount = tuple._2;
                            String[] splits = tuple._1.split("_");
                            String date = splits[0];
                            String province = splits[1];
                            String city = splits[3];
                            Long adId = Long.valueOf(splits[4]);
                            AdStat adStat = new AdStat(date,province,city,adId,clickCount);
                            adStats.add(adStat);
                        }
                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStats);

                    }
                });

            }
        });

        return aggregatedDStream;
    }


    /**
     * 生成动态黑名单
     * @param filteredAdRealTimeLogDStream
     */
    public static void generateDynamicBlacklist(JavaDStream<ConsumerRecord<String, String>> filteredAdRealTimeLogDStream){
        //计算出每5秒一个batch中 每天每个用户每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, Long>() {
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

        // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
        // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
        // 从mysql中查询
        // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
        // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化

        // 对batch中的数据，去查询mysql中的点击次数，使用哪个dstream呢？
        // dailyUserAdClickCountDStream
        // 为什么用这个batch？因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
        // 比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
        // 所以选用这个聚合后的dstream，既可以满足咱们的需求，而且呢，还可以尽量减少要处理的数据量
        // 一石二鸟，一举两得

        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                String[] splits = tuple._1.split("_");
                //变成yyyy-MM-dd
                String date = DateUtils.formatDate(DateUtils.parseDateKey(splits[0]));
                Long userid = Long.valueOf(splits[1]);
                Long adid = Long.valueOf(splits[2]);
                IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                // 从mysql中查询指定日期指定用户对指定广告的点击量
                int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid);
                // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
                // 那么就拉入黑名单，返回true
                if(clickCount >= 100) {
                    return true;
                }
                // 反之，如果点击量小于100的，那么就暂时不要管它了
                return false;
            }
        });

        // blacklistDStream
        // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
        // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
        // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
        // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
        // 所以直接插入mysql即可

        // 我们有没有发现这里有一个小小的问题？
        // blacklistDStream中，可能有userid是重复的，如果直接这样插入的话
        // 那么是不是会发生，插入重复的黑明单用户
        // 我们在插入前要进行去重
        // yyyyMMdd_userid_adid
        // 20151220_10001_10002 100
        // 20151220_10001_10003 100
        // 10001这个userid就重复了

        // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(

                new Function<Tuple2<String, Long>, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Tuple2<String, Long> tuple) throws Exception {
                        String key = tuple._1;
                        String[] keySplited = key.split("_");
                        Long userid = Long.valueOf(keySplited[1]);
                        return userid;
                    }

                });

        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
            @Override
            public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                return rdd.distinct();
            }
        });

        // 到这一步为止，distinctBlacklistUseridDStream
        // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
        distinctBlacklistUseridDStream.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
            public void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

                        while(iterator.hasNext()) {
                            long userid = iterator.next();

                            AdBlacklist adBlacklist = new AdBlacklist();
                            adBlacklist.setUserid(userid);

                            adBlacklists.add(adBlacklist);
                        }

                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                        adBlacklistDAO.insertBatch(adBlacklists);

                        // 到此为止，我们其实已经实现了动态黑名单了

                        // 1、计算出每个batch中的每天每个用户对每个广告的点击量，并持久化到mysql中

                        // 2、依据上述计算出来的数据，对每个batch中的按date、userid、adid聚合的数据
                        // 都要遍历一遍，查询一下，对应的累计的点击次数，如果超过了100，那么就认定为黑名单
                        // 然后对黑名单用户进行去重，去重后，将黑名单用户，持久化插入到mysql中
                        // 所以说mysql中的ad_blacklist表中的黑名单用户，就是动态地实时地增长的
                        // 所以说，mysql中的ad_blacklist表，就可以认为是一张动态黑名单

                        // 3、基于上述计算出来的动态黑名单，在最一开始，就对每个batch中的点击行为
                        // 根据动态黑名单进行过滤
                        // 把黑名单中的用户的点击行为，直接过滤掉

                        // 动态黑名单机制，就完成了

                        // 第一套spark课程，spark streaming阶段，有个小案例，也是黑名单
                        // 但是那只是从实际项目中抽取出来的案例而已
                        // 作为技术的学习，（案例），包装（基于你公司的一些业务），项目，找工作
                        // 锻炼和实战自己spark技术，没问题的
                        // 但是，那还不是真真正正的项目

                        // 第一个spark课程：scala、spark core、源码、调优、spark sql、spark streaming
                        // 总共加起来（scala+spark）的案例，将近上百个
                        // 搞通透，精通以后，1~2年spark相关经验，没问题

                        // 第二个spark课程（项目）：4个模块，涵盖spark core、spark sql、spark streaming
                        // 企业级性能调优、troubleshooting、数据倾斜解决方案、实际的数据项目开发流程
                        // 大数据项目架构
                        // 加上第一套课程，2~3年的spark相关经验，没问题

                    }
                });
            }
        });

    }



    /**
     * 根据黑名单进行过滤
     * @param adRealTimeLogDStream
     * @return
     */
    private static JavaDStream<ConsumerRecord<String, String>> filterByBlacklist(
            JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志之后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）

        JavaDStream<ConsumerRecord<String, String>> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transform(new Function<JavaRDD<ConsumerRecord<String, String>>, JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public JavaRDD<ConsumerRecord<String, String>> call(JavaRDD<ConsumerRecord<String, String>> recordJavaRDD) throws Exception {
                // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
                IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

                for (AdBlacklist adBlacklist : adBlacklists) {
                    tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
                }

                JavaSparkContext sc = new JavaSparkContext(recordJavaRDD.context());
                //JavaPairRDD<userid, true>
                JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                // 将原始数据rdd映射成<userid, ConsumerRecord<String, String>>

                JavaPairRDD<Long, ConsumerRecord<String, String>> mappedRDD = recordJavaRDD.mapToPair(new PairFunction<ConsumerRecord<String, String>, Long, ConsumerRecord<String, String>>() {
                    @Override
                    public Tuple2<Long, ConsumerRecord<String, String>> call(ConsumerRecord<String, String> record) throws Exception {
                        String log = record.value();
                        String[] logSplited = log.split(" ");
                        long userid = Long.valueOf(logSplited[3]);
                        return new Tuple2<>(userid, record);
                    }
                });

                // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
                // 用inner join，内连接，会导致数据丢失
                JavaPairRDD<Long, Tuple2<ConsumerRecord<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);

                JavaPairRDD<Long, Tuple2<ConsumerRecord<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<Long, Tuple2<ConsumerRecord<String, String>, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Tuple2<ConsumerRecord<String, String>, Optional<Boolean>>> tuple) throws Exception {
                        Optional<Boolean> optional = tuple._2._2;
                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                        if (optional.isPresent() && optional.get()) {
                            return false;
                        }
                        return true;
                    }
                });

                JavaRDD<ConsumerRecord<String, String>> filteredAdRealTimeLogDStream = filteredRDD.map(new Function<Tuple2<Long, Tuple2<ConsumerRecord<String, String>, Optional<Boolean>>>, ConsumerRecord<String, String>>() {
                    @Override
                    public ConsumerRecord<String, String> call(Tuple2<Long, Tuple2<ConsumerRecord<String, String>, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });

                return filteredAdRealTimeLogDStream;
            }
        });
        return filteredAdRealTimeLogDStream;
    }
}
