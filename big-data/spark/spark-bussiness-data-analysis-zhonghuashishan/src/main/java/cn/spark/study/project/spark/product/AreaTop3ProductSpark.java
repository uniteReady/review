package cn.spark.study.project.spark.product;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.ITaskDAO;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.Task;
import cn.spark.study.project.utils.ParamUtils;
import cn.spark.study.project.utils.SparkUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.xml.parsers.SAXParser;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        //1. 构造spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkSession spark = SparkUtils.getSparkSession(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //2. 生成模拟数据
        SparkUtils.mockData(jsc,spark);
        //3. 查询任务，获取任务参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if(task == null){
            System.out.println(new Date()+ " : can not find the task with id ["+ taskId+"].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        JavaPairRDD<Long,Row> cityId2ProductIdRDD = getClickProductByDate(spark, startDate, endDate);

        JavaPairRDD<Long, Row> cityId2InfoRDD = getCityInfo(spark);


        spark.stop();
    }

    /**
     * 使用spark sql 从 mysql中查询出城市信息
     * @param spark
     * @return
     */
    public static JavaPairRDD<Long,Row> getCityInfo(SparkSession spark){
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        Map<String,String > options = new HashMap<>();
        options.put("url",url);
        options.put("dbtable","city_info");
        options.put("user","root");
        options.put("password","root");
        Dataset<Row> cityInfosDS = spark.read().format("jdbc").options(options).load();
        JavaPairRDD<Long, Row> cityId2InfoRDD = cityInfosDS.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                long cityId = row.getLong(0);
                return new Tuple2<>(cityId, row);
            }
        });

        return cityId2InfoRDD;
    }

    /**
     * 使用spark sql从hive中查询出符合日期范围的用户行为日志数据
     * @param spark
     * @param startDate
     * @param endDate
     * @return
     */
    public static JavaPairRDD<Long,Row> getClickProductByDate(SparkSession spark , String startDate, String endDate){
        String sql = "select " +
                "city_id," +
                "click_product_id as product_id " +
                "from user_visit_action " +
                "where " +
                "click_product_id is not null " +
                "and click_product_id != 'NULL' " +
                "and click_product_id !='null' " +
                "and action_time >= '"+startDate+"' " +
                "and action_time <= '"+endDate +"' " ;

        Dataset<Row> clickProductDS = spark.sql(sql);
        JavaPairRDD<Long, Row> cityId2ProductIdRDD = clickProductDS.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                long cityId = row.getLong(0);
                return new Tuple2<>(cityId, row);
            }
        });
        return cityId2ProductIdRDD;

    }
}
