package cn.spark.study.project.spark.page;

import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.IPageSplitConvertRateDAO;
import cn.spark.study.project.dao.ITaskDAO;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.PageSplitConvertRate;
import cn.spark.study.project.domain.Task;
import cn.spark.study.project.utils.DateUtils;
import cn.spark.study.project.utils.NumberUtils;
import cn.spark.study.project.utils.ParamUtils;
import cn.spark.study.project.utils.SparkUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率模块spark作业
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {
        //1. 构造spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkSession spark = SparkUtils.getSparkSession(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //2. 生成模拟数据
        SparkUtils.mockData(jsc,spark);
        //3. 查询任务，获取任务参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if(task == null){
            System.out.println(new Date()+ " : can not find the task with id ["+ taskId+"].");
           return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //4. 查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParam);

        // 对用户访问行为数据做一个映射，将其映射为<sessionid,访问行为>的格式
        // 咱们的用户访问页面切片的生成，是要基于每个session的访问数据，来进行生成的
        // 脱离了session，生成的页面访问切片，是么有意义的
        // 举例，比如用户A，访问了页面3和页面5
        // 用于B，访问了页面4和页面6
        // 漏了一个前提，使用者指定的页面流筛选条件，比如页面3->页面4->页面7
        // 你能不能说，是将页面3->页面4，串起来，作为一个页面切片，来进行统计呢
        // 当然不行
        // 所以说呢，页面切片的生成，肯定是要基于用户session粒度的
        //5. 将用户访问行文数据映射成<sessionid,Row>的形式
        JavaPairRDD<String,Row> sessionid2ActionRDD = getSessionid2actionRDD(actionRDD);
        // 对<sessionid,访问行为> RDD，做一次groupByKey操作
        // 因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2ActionRDD.groupByKey();
        sessionid2actionsRDD = sessionid2actionsRDD.persist(StorageLevel.MEMORY_ONLY());
        // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
        // 生成的数据为<页面切片，1>
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(jsc, sessionid2actionsRDD, taskParam);
        //<页面切片,pvCount>
        Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();

        Long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);
        // 计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);
        persistConvertRate(taskId,convertRateMap);

        //最后 关闭spark
        spark.stop();
    }

    public static void persistConvertRate(Long taskid,Map<String, Double> convertRateMap){
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
            Double convertRate = entry.getValue();
            String pageSplit = entry.getKey();
            builder.append(pageSplit + "="+convertRate +"|");
        }
        String convertRateStr = builder.toString();
        convertRateStr=convertRateStr.substring(0,convertRateStr.length()-1);
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskid);
        pageSplitConvertRate.setConvertRate(convertRateStr);
        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

    /**
     * 计算页面单跳转换率
      * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    public static Map<String, Double> computePageSplitConvertRate(
            JSONObject taskParam,
            Map<String, Long> pageSplitPvMap,
            long startPagePv) {
        Map<String, Double> resultMap = new HashMap<>();
        String[] pageIdFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        Long lastPageSplitPv = 0L;
        for (int i = 1; i < pageIdFlow.length; i++) {
            String pageSplit = pageIdFlow[i-1]+"_"+pageIdFlow[i];
            Long pageSplitPv = pageSplitPvMap.get(pageSplit);
            Double convertRate = 0.0;
            if(i == 1){
                convertRate = NumberUtils.formatDouble(pageSplitPv * 1.0 / startPagePv,2);
            }else {
                convertRate = NumberUtils.formatDouble(pageSplitPv*1.0/lastPageSplitPv,2);
            }
            lastPageSplitPv = pageSplitPv;
            resultMap.put(pageSplit,convertRate);
        }
        return resultMap;
    }

    /**
     * 获取页面切片初始页面的点击量
     * @param taskParam
     * @param sessionid2actionsRDD
     * @return
     */
    public static Long getStartPagePv(JSONObject taskParam,JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD){
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        Long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            @Override
            public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Long> resultList = new ArrayList<>();
                Iterator<Row> it = tuple._2.iterator();
                while (it.hasNext()) {
                    Row row = it.next();
                    Long pageId = row.getLong(3);
                    if (startPageId.equals(pageId)) {
                        resultList.add(pageId);
                    }
                }
                return resultList.iterator();
            }
        });
        return startPageRDD.count();
    }

    /**
     * 将用户访问行文数据映射成<sessionid,Row>的形式
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
    }

    /**
     * 页面切片生成与匹配算法
     * @param sc
     * @param sessionid2actionsRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
            JSONObject taskParam) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return sessionid2actionsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        // 定义返回list
                        List<Tuple2<String, Integer>> list =
                                new ArrayList<Tuple2<String, Integer>>();
                        // 获取到当前session的访问行为的迭代器
                        Iterator<Row> iterator = tuple._2.iterator();
                        // 获取使用者指定的页面流
                        // 使用者指定的页面流，1,2,3,4,5,6,7
                        // 1->2的转化率是多少？2->3的转化率是多少？
                        String[] targetPages = targetPageFlowBroadcast.value().split(",");

                        // 这里，我们拿到的session的访问行为，默认情况下是乱序的
                        // 比如说，正常情况下，我们希望拿到的数据，是按照时间顺序排序的
                        // 但是问题是，默认是不排序的
                        // 所以，我们第一件事情，对session的访问行为数据按照时间进行排序

                        // 举例，反例
                        // 比如，3->5->4->10->7
                        // 3->4->5->7->10
                        // 排序

                        List<Row> rows = new ArrayList<Row>();
                        while(iterator.hasNext()) {
                            rows.add(iterator.next());
                        }

                        Collections.sort(rows, new Comparator<Row>() {

                            @Override
                            public int compare(Row o1, Row o2) {
                                String actionTime1 = o1.getString(4);
                                String actionTime2 = o2.getString(4);

                                Date date1 = DateUtils.parseTime(actionTime1);
                                Date date2 = DateUtils.parseTime(actionTime2);

                                return (int)(date1.getTime() - date2.getTime());
                            }

                        });

                        // 页面切片的生成，以及页面流的匹配
                        Long lastPageId = null;

                        for(Row row : rows) {
                            long pageid = row.getLong(3);

                            if(lastPageId == null) {
                                lastPageId = pageid;
                                continue;
                            }

                            // 生成一个页面切片
                            // 3,5,2,1,8,9
                            // lastPageId=3
                            // 5，切片，3_5

                            String pageSplit = lastPageId + "_" + pageid;

                            // 对这个切片判断一下，是否在用户指定的页面流中
                            for(int i = 1; i < targetPages.length; i++) {
                                // 比如说，用户指定的页面流是3,2,5,8,1
                                // 遍历的时候，从索引1开始，就是从第二个页面开始
                                // 3_2
                                String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];

                                if(pageSplit.equals(targetPageSplit)) {
                                    list.add(new Tuple2<String, Integer>(pageSplit, 1));
                                    break;
                                }
                            }

                            lastPageId = pageid;
                        }

                        return list.iterator();
                    }
                });
    }
}
