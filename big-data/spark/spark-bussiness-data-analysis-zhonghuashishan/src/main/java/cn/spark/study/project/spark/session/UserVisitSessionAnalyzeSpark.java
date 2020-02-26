package cn.spark.study.project.spark.session;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.*;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.*;
import cn.spark.study.project.mock.MockData;
import cn.spark.study.project.utils.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析Spark作业
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * <p>
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * 我们的spark作业如何接受用户创建的任务？
 * <p>
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * <p>
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * <p>
 * 这是spark本身提供的特性
 *
 * @author Administrator
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        args = new String[]{"1"};
        //获取sparkSession
        SparkSession spark = getSparkSession();
        //获取JavaSparkContext
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //生成模拟数据
        mockData(jsc, spark);

        //获取task的信息
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        //如果要进行Session粒度的数据聚合 首先要从user_visit_action表中查询出指定日期范围的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(spark, taskParam);

        //映射成<session_id,row> 这种键值对类型的RDD
        JavaPairRDD<String, Row> sessionIdActionRDD = getSessionId2ActionRDD(actionRDD);

        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = aggregateBySession(spark, actionRDD);

        AccumulatorV2<String,String> sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        jsc.sc().register(sessionAggrStatAccumulator,"sessionAggrStatAccumulator");

        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionId2FullAggrInfoRDD, taskParam,sessionAggrStatAccumulator);
        //获取通过筛选条件的session的访问明细数据RDD
        JavaPairRDD<String, Row> sessionId2DetailRDD = getSessionId2DetailRDD(filteredSessionid2AggrInfoRDD, sessionIdActionRDD);

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
         *
         * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
         * 再进行。。。
         *
         * 如果没有action的话，那么整个程序根本不会运行。。。
         *
         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
         * 不对！！！
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         *
         * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
         */

        randomExtractSession(taskId,filteredSessionid2AggrInfoRDD,sessionIdActionRDD);

        /**
         * 特别说明
         * 我们知道，要将上一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后
         * 才能从Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
         * 所以，我们在这里，将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
         */
        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskid());

        /**
         * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1、actionRDD，映射成<sessionid,Row>的格式
         * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4、使用自定义Accumulator中的统计值，去计算各个区间的比例
         * 5、将最后计算出来的结果，写入MySQL对应的表中
         *
         * 普通实现思路的问题：
         * 1、为什么还要用actionRDD，去映射？其实我们之前在session聚合的时候，映射已经做过了。多此一举
         * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
         * 		之前过滤session的时候，其实，就相当于，是在遍历session，那么这里就没有必要再过滤一遍了
         *
         * 重构实现思路：
         * 1、不要去生成任何新的RDD（处理上亿的数据）
         * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
         * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
         * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
         * 5、就是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达
         * 		半个小时，或者数个小时
         *
         * 开发Spark大型复杂项目的一些经验准则：
         * 1、尽量少生成RDD
         * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
         * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
         * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
         * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
         * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
         * 4、无论做什么功能，性能第一
         * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，我认为是架构和可维护性，可扩展性的重要
         * 		程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
         *
         * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，我认为性能的重要程度，远远大于一些代码
         * 		的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能
         * 		主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢
         * 		如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
         * 		此时，对于用户体验，简直就是一场灾难
         *
         * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
         *
         * 		我们如果采用第一种实现方案，那么其实就是代码划分（解耦合、可维护）优先，设计优先
         * 		如果采用第二种方案，那么其实就是性能优先
         *
         * 		讲了这么多，其实大家不要以为我是在岔开话题，大家不要觉得项目的课程，就是单纯的项目本身以及
         * 		代码coding最重要，其实项目，我觉得，最重要的，除了技术本身和项目经验以外；非常重要的一点，就是
         * 		积累了，处理各种问题的经验
         *
         */

        //获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryCountList = getTop10Category(task.getTaskid(), sessionId2DetailRDD);

        //获取top10 活跃session
        getTop10Session(jsc,task.getTaskid(),sessionId2DetailRDD,top10CategoryCountList);



        //关闭spark上下文
        spark.stop();

    }

    /**
     * 获取top10 活跃session
     * @param jsc  javaSparkContext
     * @param taskid taskId
     * @param sessionId2DetailRDD  JavaPairRDD<sessionId,访问明细></>
     * @param top10CategoryCountList top10热门品类id的list
     */
    private static void getTop10Session(JavaSparkContext jsc,long taskid, JavaPairRDD<String, Row> sessionId2DetailRDD,List<Tuple2<CategorySortKey, String>> top10CategoryCountList) {

        /**
         * 第一步： 将top10 热门品类的id 生成一份RDD
         */
        List<Tuple2<String,String>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> category : top10CategoryCountList) {
            String categoryId = StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID);
            top10CategoryIdList.add(new Tuple2<>(categoryId,categoryId));
        }
        JavaPairRDD<String, String> top10CategoryIdRDD = jsc.parallelizePairs(top10CategoryIdList);
        /**
         * 第二步：计算top10品类被各个session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionId2DetailsRDD = sessionId2DetailRDD.groupByKey();
        JavaPairRDD<String, String> categoryId2SessionIdCountRDD = sessionId2DetailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterator<Row> it = tuple._2.iterator();
                Map<String, Long> categoryId2CountMap = new HashMap<>();
                while (it.hasNext()) {
                    Row row = it.next();
                    String categoryId = row.getString(6);
                    if (StringUtils.isNotEmpty(categoryId)) {
                        Long count = categoryId2CountMap.get(categoryId);
                        if (count == null) {
                            count = 0L;
                        }
                        count++;
                        categoryId2CountMap.put(categoryId, count);
                    }
                }
                //返回结果： Tuple2<categoryId,  sessionId,count>
                List<Tuple2<String, String>> resultList = new ArrayList<>();
                for (Map.Entry<String, Long> entry : categoryId2CountMap.entrySet()) {
                    String categoryId = entry.getKey();
                    Long count = entry.getValue();
                    String value = sessionId + "," + count;
                    resultList.add(new Tuple2<>(categoryId, value));
                }
                return resultList.iterator();
            }
        });
        //获取到top10热门品类 被各个session点击的次数  JavaPairRDD<categoryId, sessionId,Count>
        JavaPairRDD<String,String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryId2SessionIdCountRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
            @Override
            public Tuple2<String,String> call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
                return new Tuple2<>(tuple._1,tuple._2._2);
            }
        });

        /**
         * 第三步：分组取topN算法实现 获取每个品类的top10 活跃用户
         */
        JavaPairRDD<String, Iterable<String>> top10CategorySessionCountsRDD = (JavaPairRDD<String, Iterable<String>>) top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD= top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String categoryId = tuple._1;
                Iterator<String> it = tuple._2.iterator();
                // 定义取topn的排序数组
                String[] top10Sessions = new String[10];
                while (it.hasNext()) {
                    String sessionCount = it.next();
                    String[] splits = sessionCount.split(",");
                    String sessionId = splits[0];
                    Long count = Long.valueOf(splits[1]);
                    // 遍历排序数组
                    for (int i = 0; i < top10Sessions.length; i++) {
                        // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
                        if (top10Sessions[i] == null) {
                            top10Sessions[i] = sessionCount;
                            break;
                        } else {
                            Long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                            // 如果sessionCount比i位的sessionCount要大
                            if (count > _count) {
                                for (int j = top10Sessions.length; j > i; j--) {
                                    top10Sessions[j] = top10Sessions[j - 1];
                                }
                                // 将i位赋值为sessionCount
                                top10Sessions[i] = sessionCount;
                                break;
                            }
                            // 如果sessionCount比i位的sessionCount要小，继续外层for循环
                        }
                    }
                }
                //List<Tuple2<sessionId,sessionId>>
                List<Tuple2<String,String>> resultList = new ArrayList<>();
                ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                for (String sessionCount : top10Sessions) {
                    if(sessionCount !=null){
                        String sessionId = sessionCount.split(",")[0];
                        Long count = Long.valueOf(sessionCount.split(",")[1]);
                        // 将数据写入MySQL表
                        Top10Session top10Session = new Top10Session(taskid,categoryId,sessionId,count);
                        top10SessionDAO.insert(top10Session);
                        // 放入list
                        resultList.add(new Tuple2<>(sessionId,sessionId));
                    }
                }

                return resultList.iterator();
            }
        });

        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD.join(sessionId2DetailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getString(6));
                sessionDetail.setClickProductId(row.getString(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });

    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionIdActionRDD
     * @return
     */
    public  static JavaPairRDD<String,Row> getSessionId2DetailRDD(JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionIdActionRDD){
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionIdActionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                return new Tuple2<>(tuple._1, tuple._2._2);
            }
        });

        return sessionid2detailRDD;
    }


    /**
     * 获取top10 热门品类
     * @param taskId
     * @param sessionId2DetailRDD
     */
    public static List<Tuple2<CategorySortKey, String>> getTop10Category(Long taskId,JavaPairRDD<String, Row> sessionId2DetailRDD) {
         //获取符合条件的session访问过的所有品类id
        //访问过:指的是,点击过或者下单过或者支付过的都算访问过
        JavaPairRDD<String, String> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Row> tuple) throws Exception {
                Set<Tuple2<String, String>> resultset = new HashSet<>();
                Row row = tuple._2;
                //有点击过的
                String clickCategoryId = row.getString(6);
                if (StringUtils.isNotEmpty(clickCategoryId)) {
                    resultset.add(new Tuple2<>(clickCategoryId, clickCategoryId));

                }
                //有下过单的
                String orderCategoryIds = row.getString(8);
                if (StringUtils.isNotEmpty(orderCategoryIds)) {
                    String[] splits = orderCategoryIds.split(",");
                    for (String orderCategoryId : splits) {
                        resultset.add(new Tuple2<>(orderCategoryId, orderCategoryId));
                    }
                }
                //有支付过的
                String payCategoryIds = row.getString(10);
                if (StringUtils.isNotEmpty(payCategoryIds)) {
                    String[] splits = payCategoryIds.split(",");
                    for (String payCategoryId : splits) {
                        resultset.add(new Tuple2<>(payCategoryId, payCategoryId));
                    }
                }
                return resultset.iterator();
            }
        });
        /**
         * 这里一定要对categoryId进行去重
         */
        categoryIdRDD=categoryIdRDD.distinct();

        /**
         * 第二步:计算各品类的点击 下单和支付的次数
         */
       JavaPairRDD<String,Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2DetailRDD);
       JavaPairRDD<String,Long> orderCategoryId2CountRDD = getorderCategoryId2CountRDD(sessionId2DetailRDD);
       JavaPairRDD<String,Long> payCategoryId2CountRDD = getpayCategoryId2CountRDD(sessionId2DetailRDD);
        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
         *
         * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
         * 只不过，没有join到的那个数据，就是0了
         *
         */
        //这一步返回的JavaPairRDD<String,String> 是 JavaPairRDD<品类id,该id的点击 下单 支付的次数 以键值对的String形式>
       JavaPairRDD<String,String> categoryid2countRDD = joinCategoryAndData(categoryIdRDD,clickCategoryId2CountRDD,orderCategoryId2CountRDD,payCategoryId2CountRDD);


        /**
         * 第四步：自定义二次排序key
         */

        /**
         * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<String, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<String, String> tuple) throws Exception {
                String categoryIdInfo = tuple._2;
                Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_PAY_COUNT));
                CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
                return new Tuple2<>(categorySortKey, categoryIdInfo);
            }
        });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);
        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryCountList = sortedCategoryCountRDD.take(10);
        ITop10CategoryDAO top10CategoryDao = DAOFactory.getTop10CategoryDao();
        for (Tuple2<CategorySortKey, String> tuple : top10CategoryCountList) {
            String categoryIdInfo= tuple._2;
            String categoryId = StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_CATEGORY_ID);
            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(categoryIdInfo, "\\|", Constants.FIELD_PAY_COUNT));
            Top10Category top10Category = new Top10Category(taskId,categoryId,clickCount,orderCount,payCount);
            top10CategoryDao.insert(top10Category);
        }

        return top10CategoryCountList;

    }

    /**
     * 连接品类RDD与数据RDD
     * @param categoryIdRDD  用户有访问过的品类id的RDD  JavaPairRDD<品类id, 品类id>
     * @param clickCategoryId2CountRDD 用户点击过的品类id和访问次数的RDD JavaPairRDD<品类id, count>
     * @param orderCategoryId2CountRDD 用户下过单的品类id和访问次数的RDD JavaPairRDD<品类id, count>
     * @param payCategoryId2CountRDD 用户支付过的品类id和访问次数的RDD JavaPairRDD<品类id, count>
     * @return
     */
    private static JavaPairRDD<String, String> joinCategoryAndData(JavaPairRDD<String, String> categoryIdRDD,
                                                                 JavaPairRDD<String, Long> clickCategoryId2CountRDD,
                                                                 JavaPairRDD<String, Long> orderCategoryId2CountRDD,
                                                                 JavaPairRDD<String, Long> payCategoryId2CountRDD) {
        // 解释一下，如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
        // 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
        JavaPairRDD<String, Tuple2<String, Optional<Long>>> tmpJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);
        JavaPairRDD<String, String> tmpMapRDD = tmpJoinRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Long>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                String categoryId = tuple._1;
                Optional<Long> optional = tuple._2._2;
                Long clickCount= 0L;
                if(optional.isPresent()){
                    clickCount = optional.get();
                }
                String value =Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"+ Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                return new Tuple2<>(categoryId,value);
            }
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Long>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                String categoryId = tuple._1;
                Optional<Long> optional = tuple._2._2;
                String value = tuple._2._1;
                Long orderCount = 0L ;
                if(optional.isPresent()){
                    orderCount = optional.get();
                }
                value = value +"|"+Constants.FIELD_ORDER_COUNT + "="+orderCount ;

                return new Tuple2<>(categoryId,value);
            }
        });
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Long>>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                String categoryId = tuple._1;
                Optional<Long> optional = tuple._2._2;
                String value = tuple._2._1;
                Long payCount = 0L ;
                if(optional.isPresent()){
                    payCount = optional.get();
                }
                value = value +"|"+Constants.FIELD_PAY_COUNT + "="+payCount ;

                return new Tuple2<>(categoryId,value);
            }
        });

        return tmpMapRDD;
    }

    /**
     * 计算各品类的支付的次数 就是一个wordCount
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<String, Long> getpayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                String payCategoryIds = tuple._2.getString(10);
                return payCategoryIds != null;
            }
        });

        JavaPairRDD<String, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, String, Long>() {
            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                String payCategoryIds = tuple._2.getString(10);
                String[] splits = payCategoryIds.split(",");
                List<Tuple2<String, Long>> resultList = new ArrayList<>();
                for (String payCategoryId : splits) {
                    resultList.add(new Tuple2<>(payCategoryId, 1L));
                }
                return resultList.iterator();
            }
        });
        JavaPairRDD<String, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return payCategoryId2CountRDD;
    }

    /**
     * 计算各品类的下单的次数 就是一个wordCount
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<String, Long> getorderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                String orderCategoryId = tuple._2.getString(8);
                return orderCategoryId != null;
            }
        });
        JavaPairRDD<String, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, String, Long>() {
            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                String orderCategoryIds = tuple._2.getString(8);
                String[] splits = orderCategoryIds.split(",");
                List<Tuple2<String, Long>> resultList = new ArrayList<>();
                for (String orderCategoryId : splits) {
                    resultList.add(new Tuple2<>(orderCategoryId, 1L));
                }
                return resultList.iterator();
            }
        });

        JavaPairRDD<String, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return orderCategoryId2CountRDD;
    }

    /**
     * 计算各品类的点击的次数 就是一个wordCount
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<String, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                String clickCategoryId = row.getString(6);
                return clickCategoryId != null;
            }
        });
        JavaPairRDD<String, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Row> tuple) throws Exception {
                String clickCategoryId = tuple._2.getString(6);
                return new Tuple2<>(clickCategoryId, 1L);
            }
        });
        JavaPairRDD<String, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return clickCategoryId2CountRDD;
    }

    /**
     * 随机抽取session
     * @param filteredSessionid2AggrInfoRDD
     */
    public static void randomExtractSession(Long taskId,
                                            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                            JavaPairRDD<String, Row> sessionIdActionRDD){
        JavaPairRDD<String, String> time2sessionidRDD = filteredSessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                String fullAggrInfo = tuple._2;
                String startTimeStr = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME);
                String startDateHour = DateUtils.getDateHour(startTimeStr);
                return new Tuple2<>(startDateHour, fullAggrInfo);
            }
        });
        /**
         * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
         *
         * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
         * 首先抽取出的session的聚合数据，写入session_random_extract表
         * 所以第一个RDD的value，应该是session聚合数据
         *
         */

        // 得到每天每小时的session数量
        Map<String, Long> countMap = time2sessionidRDD.countByKey();

        // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String,Map<String,Long>> dateHourCountMap = new HashMap<>();
        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
            Long count = entry.getValue();
            String dateHour = entry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null){
                hourCountMap = new HashMap<>();
            }
            hourCountMap.put(hour,count);
            dateHourCountMap.put(date,hourCountMap);
        }

        // 开始实现我们的按时间比例随机抽取算法

        //假设总共要抽取100个session,先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date,<hour,(3,5,20,102)>>  <日期,<小时,List<索引>>>
        Map<String,Map<String, List<Integer>>> dateHourExtractMap= new HashMap<>();

        Random random = new Random();
        Long sessionCount = 0L;
        for (Map.Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
            String date = entry.getKey();
            Map<String, Long> hourCountMap = entry.getValue(); //当天的每小时的session count数
            //获取每天的总session数量
            for (Long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }
            //至此 拿到了每天的session的总数量

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null){
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date,hourExtractMap);
            }

            // 遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                //计算这个小时的session的数量占了当天总数的百分比  然后乘以平均每天要采集的session个数 就可以算出这个小时需要抽取的session的个数
                int hourExtractNumber = (int) ((double) count * extractNumberPerDay / (double) sessionCount);
                if(hourExtractNumber >= count){
                    hourExtractNumber=(int) count;
                }
                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if(extractIndexList == null){
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour,extractIndexList);
                }

                // 生成上面计算出来的数量的随机数 也就是要抽取的数据的索引
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)){
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }

            }

        }
        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */
        //拿到的是<dateHour,Iterable<fullAggrInfo>>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        // 我们用flatMap算子，遍历所有的<dateHour,Iterable<fullAggrInfo>>格式的数据
        // 然后呢，会遍历每天每小时的session
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        // 那么抽取该session，直接写入MySQL的random_extract_session表
        // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
        //因为这里后面要进行join 所以这里用了flatMapToPair而不是单纯用flatMap
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                List<Tuple2<String, String>> resultList = new ArrayList<>();
                String dateHour = tuple._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];
                List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
                Iterator<String> it = tuple._2.iterator();
                int index = 0;
                while (it.hasNext()) {
                    String fullAggrInfo = it.next();
                    if (extractIndexList.contains(index)) {
                        String sessionId = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                        SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                        sessionRandomExtract.setTaskid(taskId);
                        sessionRandomExtract.setSessionid(sessionId);
                        sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME));
                        sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                        sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();
                        sessionRandomExtractDAO.insert(sessionRandomExtract);
                        resultList.add(new Tuple2<>(sessionId, sessionId));
                    }
                    index++;
                }
                return resultList.iterator();
            }
        });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD.join(sessionIdActionRDD);
        extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskId);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(tuple._1);
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getString(6));
                sessionDetail.setClickProductId(row.getString(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });


    }


    /**
     * 计算各session范围占比，并写入MySQL
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        System.out.println(value);
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * 过滤session数据 并进行统计
     *
     * @param sessionId2FullAggrInfoRDD session粒度的全信息的RDD <sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
     * @param taskParam                 任务的参数
     * @return
     */
    public static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionId2FullAggrInfoRDD,
                                                                       JSONObject taskParam,
                                                                       AccumulatorV2<String,String> sessionAggrStatAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionId2FullAggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String fullAggrInfo = tuple._2;
                // 接着，依次按照筛选条件进行过滤
                // 按照年龄范围进行过滤（startAge、endAge）
                if (!ValidUtils.between(fullAggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }
                // 按照性别进行过滤
                // 男/女
                // 男，女
                if (!ValidUtils.equal(fullAggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }
                // 按照城市范围进行过滤（cities）
                // 北京,上海,广州,深圳
                // 成都
                if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }
                // 按照职业范围进行过滤（professionals）
                // 互联网,IT,软件
                // 互联网
                if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }
                // 按照搜索词进行过滤
                // 我们的session可能搜索了 火锅,蛋糕,烧烤
                // 我们的筛选条件可能是 火锅,串串香,iphone手机
                // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                // 任何一个搜索词相当，即通过
                if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }
                // 按照点击品类id进行过滤
                if (!ValidUtils.in(fullAggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }
                // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                // 进行相应的累加计数

                // 只要走到这一步，那么就是需要计数的session
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                //计算出session的访问时长和访问步长的范围 并进行相应的累加
                Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);


                return true;
            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * @param spark     sparkSession
     * @param actionRDD 用户访问行为表中的按照时间范围查询出来的行为数据
     * @return
     */
    public static JavaPairRDD<String, String> aggregateBySession(SparkSession spark, JavaRDD<Row> actionRDD) {
        //映射成<session_id,row> 这种键值对类型的RDD
        JavaPairRDD<String, Row> sessionIdActionRDD = getSessionId2ActionRDD(actionRDD);

        //按照session_id进行分组得到session粒度的数据
        JavaPairRDD<String, Iterable<Row>> sessionId2ActionRDD = sessionIdActionRDD.groupByKey();
        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = sessionId2ActionRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String session_id = tuple._1;
                Iterator<Row> it = tuple._2.iterator();
                StringBuilder searchKeyWordsBuilder = new StringBuilder();
                StringBuilder clickCategoryIdsBuilder = new StringBuilder();
                //session的访问开始时间和结束时间
                Date startTime = null;
                Date endTime = null;
                //session的访问步长
                Integer stepLength = 0;
                Long userId = null;
                while (it.hasNext()) {
                    // 提取每个访问行为的搜索词字段和点击品类字段
                    Row row = it.next();
                    //提取用户id
                    if (null == userId) {
                        userId = row.getLong(1);
                    }
                    String searchKeyWord = row.getString(5);
                    //TODO tianyafu 这里不能直接用row.getLong(6)  因为getLong如果下标为6的value为null的话就会抛空指针异常，
                    // 也不能Long clickCategoryId = row.<Long>getAs(6);这么写，如果这么写的话，一旦value为null getAs[Long] 会把null改为0，
                    // 所以这里统一用getString来做，相应的ClickCategoryId这个生成的数据的类型也要全部改为String  这样是不会出错的
//                    Long clickCategoryId = row.getLong(6);
                    String clickCategoryId = row.getString(6);
                    // 实际上这里要对数据说明一下
                    // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                    // 其实，只有搜索行为，是有searchKeyword字段的
                    // 只有点击品类的行为，是有clickCategoryId字段的
                    // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                    // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                    // 首先要满足：不能是null值
                    // 其次，之前的字符串中还没有搜索词或者点击品类id
                    if (null != searchKeyWord && !searchKeyWordsBuilder.toString().contains(searchKeyWord)) {
                        searchKeyWordsBuilder.append(searchKeyWord + ",");
                    }
                    if (null != clickCategoryId && !clickCategoryIdsBuilder.toString().contains(String.valueOf(clickCategoryId))) {
                        clickCategoryIdsBuilder.append(clickCategoryId + ",");
                    }
                    //计算并设置session开始时间和结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if (startTime == null) {
                        startTime = actionTime;
                    }
                    if (endTime == null) {
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }
                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                    //计算访问步长
                    stepLength++;


                }
                //计算访问时长(单位s)
                Integer visitLength = DateUtils.minus(DateUtils.formatTime(endTime), DateUtils.formatTime(startTime));
                String searchKeywords = StringUtils.trimComma(searchKeyWordsBuilder.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuilder.toString());
                // 大家思考一下
                // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                // 然后再直接将返回的Tuple的key设置成sessionid
                // 最后的数据格式，还是<sessionid,fullAggrInfo>

                // 聚合数据，用什么样的格式进行拼接？
                // 我们这里统一定义，使用key=value|key=value
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + session_id + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                        Constants.FIELD_STEP_LENGTH + "=" + stepLength +"|"+
                        Constants.FIELD_START_TIME + "="+ DateUtils.formatTime(startTime);
                return new Tuple2<>(userId, partAggrInfo);
            }
        });
        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        Dataset<Row> userInfoDS = spark.sql(sql);
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoDS.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple2) throws Exception {
                String partAggrInfo = tuple2._2._1;
                Row userInfoRow = tuple2._2._2;

                String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                Integer age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                String fullAggrInfo = partAggrInfo + "|" +
                        Constants.FIELD_AGE + "=" + age + "|" +
                        Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                        Constants.FIELD_CITY + "=" + city + "|" +
                        Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionId, fullAggrInfo);
            }
        });

        return sessionId2FullAggrInfoRDD;

    }

    public static JavaPairRDD<String, Row>  getSessionId2ActionRDD(JavaRDD<Row> actionRDD){
        //映射成<session_id,row> 这种键值对类型的RDD
        JavaPairRDD<String, Row> sessionIdActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
        return sessionIdActionRDD;
    }

    /**
     * 要从user_visit_action表中查询出指定日期范围的行为数据
     *
     * @param spark     sparkSession
     * @param taskParam 任务的参数
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'";
        Dataset<Row> actionDS = spark.sql(sql);
        return actionDS.javaRDD();
    }

    /**
     * 获取SparkSession
     *
     * @return
     */
    public static SparkSession getSparkSession() {
        SparkSession.Builder builder = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).master("local");
        //如果不是本地模式  我们让他开启hive支持
        if (!ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            builder = builder.enableHiveSupport();
        }
        return builder.getOrCreate();
    }

    /**
     * 生成模拟数据，只有在本地模式下才去生成数据
     *
     * @param sc    sparkContext
     * @param spark sparkSession
     */
    public static void mockData(JavaSparkContext sc, SparkSession spark) {
        if (ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            MockData.mock(sc, spark);
        }

    }


}
