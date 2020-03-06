package cn.spark.study.project.spark.product;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.dao.IAreaTop3ProductDAO;
import cn.spark.study.project.dao.ITaskDAO;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.AreaTop3Product;
import cn.spark.study.project.domain.Task;
import cn.spark.study.project.utils.ParamUtils;
import cn.spark.study.project.utils.SparkUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import javax.xml.parsers.SAXParser;
import java.util.*;

public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        //1. 构造spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkSession spark = SparkUtils.getSparkSession(conf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //1.1 注册UDF
        spark.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        //注册UDAF
        spark.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());

        //2. 生成模拟数据
        SparkUtils.mockData(jsc, spark);
        //3. 查询任务，获取任务参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if (task == null) {
            System.out.println(new Date() + " : can not find the task with id [" + taskId + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        //使用spark sql从hive中查询出符合日期范围的用户行为日志数据
        JavaPairRDD<Long, Row> cityId2ProductIdRDD = getcityId2ProductIdByDate(spark, startDate, endDate);
        //使用spark sql 从 mysql中查询出城市信息
        JavaPairRDD<Long, Row> cityId2InfoRDD = getCityId2Info(spark);
        try {
            //生成点击商品基础信息临时表
            generateTempClickProductBasicTable(spark, cityId2ProductIdRDD, cityId2InfoRDD);
            //生成各区域各商品点击次数临时表
            generateTempAreaProductClickCountTable(spark);
            //生成区域商品点击次数临时表（包含了商品的完整信息）
            generateTempAreaFullProductClickCountTable(spark);
            //获取各区域top3热门商品
            JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(spark);
            List<Row> areaTop3ProductList = areaTop3ProductRDD.collect();
            //将计算出来的各区域top3热门商品写入MySQL中
            persistAreaTop3Product(taskId,areaTop3ProductList);

        } catch (AnalysisException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


        spark.stop();
    }

    /**
     * 将计算出来的各区域top3热门商品写入MySQL中
     * @param rows
     */
    private static void persistAreaTop3Product(long taskid, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();

        for(Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areTop3ProductDAO.insertBatch(areaTop3Products);
    }

    /**
     * 获取各区域top3热门商品
     *
     * @param spark
     */
    public static JavaRDD<Row> getAreaTop3ProductRDD(SparkSession spark) throws AnalysisException {
        // 技术点：开窗函数

        // 使用开窗函数先进行一个子查询
        // 按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
        // 接着在外层查询中，过滤出各个组内的行号排名前3的数据
        // 其实就是咱们的各个区域下top3热门商品

        // 华北、华东、华南、华中、西北、西南、东北
        // A级：华北、华东
        // B级：华南、华中
        // C级：西北、西南
        // D级：东北

        // case when
        // 根据多个条件，不同的条件对应不同的值
        // case when then ... when then ... else ... end
        /*String sql = "select " +
                "area,"
                + "CASE "
                + "WHEN area='China North' OR area='China East' THEN 'A Level' "
                + "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
                + "WHEN area='West North' OR area='West South' THEN 'C Level' "
                + "ELSE 'D Level' "
                + "END area_level," +
                "product_id," +
                "click_count," +
                "city_infos," +
                "product_name," +
                "product_status " +
                "from " +
                "(" +
                "select " +
                "area," +
                "product_id," +
                "click_count," +
                "city_infos," +
                "product_name," +
                "product_status，row_number() over (partition by area order by click_count desc) rank " +
                "from  " +
                "tmp_area_fullprod_click_count ) t " +
                "where t.rank <=3";*/

        String sql =
                "SELECT "
                        + "area,"
                        + "CASE "
                        + "WHEN area='China North' OR area='China East' THEN 'A Level' "
                        + "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
                        + "WHEN area='West North' OR area='West South' THEN 'C Level' "
                        + "ELSE 'D Level' "
                        + "END area_level,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status "
                        + "FROM ("
                        + "SELECT "
                        + "area,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status,"
                        + "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
                        + "FROM tmp_area_fullprod_click_count "
                        + ") t "
                        + "WHERE rank<=3";
        Dataset<Row> ds = spark.sql(sql);
        return ds.javaRDD();
    }

    /**
     * 生成区域商品点击次数临时表（包含了商品的完整信息）
     *
     * @param spark
     */
    public static void generateTempAreaFullProductClickCountTable(SparkSession spark) throws AnalysisException {
        String sql = "select " +
                "tapcc.area," +
                "tapcc.product_id," +
                "tapcc.click_count," +
                "tapcc.city_infos," +
                "pi.product_name," +
                "if(get_json_object(pi.extend_info,'product_status')='0','自营','第三方') product_status " +
                "from tmp_area_product_click_count tapcc join product_info pi " +
                "on tapcc.product_id = pi.product_id";

        Dataset<Row> ds = spark.sql(sql);
        ds.createTempView("tmp_area_fullprod_click_count");


    }

    /**
     * 生成各区域各商品点击次数临时表
     *
     * @param spark
     * @throws AnalysisException
     */
    public static void generateTempAreaProductClickCountTable(SparkSession spark) throws AnalysisException {
        String sql = "select " +
                "area," +
                "product_id," +
                "count(1) click_count ," +
                "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
                "from tmp_click_product_basic " +
                "group by area,product_id ";
        Dataset<Row> ds = spark.sql(sql);
        ds.createTempView("tmp_area_product_click_count");

    }

    /**
     * 生成点击商品基础信息临时表
     *
     * @param spark
     * @param cityId2ProductIdRDD
     * @param cityId2InfoRDD
     * @throws AnalysisException
     */
    public static void generateTempClickProductBasicTable(SparkSession spark, JavaPairRDD<Long, Row> cityId2ProductIdRDD, JavaPairRDD<Long, Row> cityId2InfoRDD) throws AnalysisException {
        //将用户点击数据与城市信息数据join起来
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityId2ProductIdRDD.join(cityId2InfoRDD);

        JavaRDD<Row> mapedRDD = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                Long cityId = tuple._1;
                Row productIdRow = tuple._2._1;
                Row cityInfoRow = tuple._2._2;
                long productId = Long.valueOf(productIdRow.getString(1));
                String cityName = cityInfoRow.getString(1);
                String area = cityInfoRow.getString(2);

                return RowFactory.create(cityId, productId, cityName, area);
            }
        });
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> ds = spark.createDataFrame(mapedRDD, structType);
        // 将DataFrame中的数据，注册成临时表（tmp_click_product_basic）
        ds.createTempView("tmp_click_product_basic");

    }


    /**
     * 使用spark sql 从 mysql中查询出城市信息
     *
     * @param spark
     * @return
     */
    public static JavaPairRDD<Long, Row> getCityId2Info(SparkSession spark) {
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String username = ConfigurationManager.getProperty(Constants.JDBC_USERNAME);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        Map<String, String> options = new HashMap<>();
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", username);
        options.put("password", password);
        Dataset<Row> cityInfosDS = spark.read().format("jdbc").options(options).load();
        JavaPairRDD<Long, Row> cityId2InfoRDD = cityInfosDS.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                long cityId = Long.valueOf(String.valueOf(row.get(0)));
                return new Tuple2<>(cityId, row);
            }
        });

        return cityId2InfoRDD;
    }

    /**
     * 使用spark sql从hive中查询出符合日期范围的用户行为日志数据
     *
     * @param spark
     * @param startDate
     * @param endDate
     * @return
     */
    public static JavaPairRDD<Long, Row> getcityId2ProductIdByDate(SparkSession spark, String startDate, String endDate) {
        String sql = "select " +
                "city_id," +
                "click_product_id as product_id " +
                "from user_visit_action " +
                "where " +
                "click_product_id is not null " +
                "and click_product_id !='null' " +
                "and date >= '" + startDate + "' " +
                "and date <= '" + endDate + "' ";

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
