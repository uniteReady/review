package cn.spark.study.project.spark;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.ITaskDAO;
import cn.spark.study.project.dao.impl.DAOFactory;
import cn.spark.study.project.domain.Task;
import cn.spark.study.project.mock.MockData;
import cn.spark.study.project.utils.ParamUtils;
import cn.spark.study.project.utils.StringUtils;
import cn.spark.study.project.utils.ValidUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.tools.nsc.doc.model.Val;

import java.util.Iterator;

/**
 * 用户访问session分析Spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * 我们的spark作业如何接受用户创建的任务？
 *
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 *
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * 这是spark本身提供的特性
 *
 * @author Administrator
 *
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        args = new String[]{"1"};
        //获取sparkSession
        SparkSession spark= getSparkSession();
        //获取JavaSparkContext
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //生成模拟数据
        mockData(jsc,spark);

        //获取task的信息
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        //如果要进行Session粒度的数据聚合 首先要从user_visit_action表中查询出指定日期范围的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(spark, taskParam);
        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = aggregateBySession(spark, actionRDD);

        // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于我们自己编写的算子，是要访问外面的任务参数对象的
        // 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSession(sessionId2FullAggrInfoRDD, taskParam);


        //关闭spark上下文
        spark.stop();

    }

    /**
     * 过滤session数据
     * @param sessionId2FullAggrInfoRDD  session粒度的全信息的RDD <sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
     * @param taskParam  任务的参数
     * @return
     */
    public static JavaPairRDD<String,String> filterSession(JavaPairRDD<String, String> sessionId2FullAggrInfoRDD,JSONObject taskParam){
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
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter=_parameter;

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

                return true;
            }
        });
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     *
     * @param spark sparkSession
     * @param actionRDD 用户访问行为表中的按照时间范围查询出来的行为数据
     * @return
     */
    public static JavaPairRDD<String,String> aggregateBySession(SparkSession spark,JavaRDD<Row> actionRDD){
        //映射成<session_id,row> 这种键值对类型的RDD
        JavaPairRDD<String, Row> sessionIdActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });

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

                }
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
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
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

                String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|" ,Constants.FIELD_SESSION_ID);
                Integer age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);
                String fullAggrInfo = partAggrInfo+"|"+
                        Constants.FIELD_AGE+"="+age+"|"+
                        Constants.FIELD_PROFESSIONAL+"="+professional+"|"+
                        Constants.FIELD_CITY+"="+city+"|"+
                        Constants.FIELD_SEX+"="+sex;

                return new Tuple2<>(sessionId,fullAggrInfo);
            }
        });

        return sessionId2FullAggrInfoRDD;

    }

    /**
     * 要从user_visit_action表中查询出指定日期范围的行为数据
     * @param spark sparkSession
     * @param taskParam 任务的参数
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark,JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'";
        Dataset<Row> actionDS = spark.sql(sql);
        return actionDS.javaRDD();
    }

    /**
     * 获取SparkSession
     * @return
     */
    public static SparkSession getSparkSession(){
        SparkSession.Builder builder = SparkSession.builder().appName(Constants.SPARK_APP_NAME_SESSION).master("local");
        //如果不是本地模式  我们让他开启hive支持
        if (!ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            builder = builder.enableHiveSupport();
        }
        return builder.getOrCreate();
    }

    /**
     * 生成模拟数据，只有在本地模式下才去生成数据
     * @param sc sparkContext
     * @param spark sparkSession
     */
    public static void mockData(JavaSparkContext sc,SparkSession spark){
        if (ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            MockData.mock(sc,spark);
        }

    }


}
