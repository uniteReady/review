package cn.spark.study.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class DailyTop3KeyWord {

    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().master("local").appName("DailyTop3KeyWord").getOrCreate();
        JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
        //这里需要伪造一份数据作为查询条件
        Map<String, List<String>> queryParamMap = new HashMap<>();

        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0","1.2","1.5","2.0"));

        //优化：将查询条件queryParamMap广播出去，这样每个worker只有一份
        Broadcast<Map<String, List<String>>> queryParamMapBroadcast = jssc.broadcast(queryParamMap);

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\searchWords.txt";
        JavaRDD<String> searchLogLineRDD = jssc.textFile(path);

        //使用查询条件广播变量进行数据筛选
        JavaRDD<String> filterRDD = searchLogLineRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                String[] splits = line.split(" ");
                String city = splits[3];
                String platform = splits[4];
                String version = splits[5];
                Map<String, List<String>> queryParamMap = queryParamMapBroadcast.value();
                if(queryParamMap.size()>0 && !queryParamMap.get("city").contains(city)){
                    return false;
                }
                if(queryParamMap.size()>0 && !queryParamMap.get("platform").contains(platform)){
                    return false;
                }
                if(queryParamMap.size()>0 && !queryParamMap.get("version").contains(version)){
                    return false;
                }
                return true;
            }
        });
        //将过滤出来的原始日志，映射为（日期_搜索词,用户）的格式
        JavaPairRDD<String, String> dateKeyWordUserRDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] splits = line.split(" ");
                return new Tuple2<>(splits[0] + "_" + splits[2], splits[1]);

            }
        });
        //进行分组，获取每天每个搜索词，有哪些用户搜索了（没有去重）
        JavaPairRDD<String, Iterable<String>> dateKeyWordUsersRDD = dateKeyWordUserRDD.groupByKey();
        //对每天每个搜索词得到的搜索用户进行去重操作，获取其uv  格式：（日期_搜索词,uv）
        JavaPairRDD<String, Long> dateKeyWordUvRDD = dateKeyWordUsersRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeyWordUsers) throws Exception {
                String dateKeyWord = dateKeyWordUsers._1;
                Iterator<String> users = dateKeyWordUsers._2.iterator();
                Set<String> userSet = new HashSet<>();
                while (users.hasNext()){
                    userSet.add(users.next());
                }
                return new Tuple2<>(dateKeyWord,Long.valueOf(userSet.size()));
            }
        });

        //将每天每个搜索词的uv转换成DataFrame Row(日期，搜索词，uv)
        JavaRDD<Row> dateKeyWordUVRowRDD = dateKeyWordUvRDD.map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> v1) throws Exception {
                return RowFactory.create(v1._1.split("_")[0], v1._1.split("_")[1], v1._2);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyWord", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dateKeyWordUvDs = spark.createDataFrame(dateKeyWordUVRowRDD, structType);

        dateKeyWordUvDs.createTempView("daily_keyword_uv");
        Dataset<Row> dateKeyWordTop3UvDS = spark.sql("select " +
                " date,keyWord,uv " +
                " from (" +
                "select " +
                " date," +
                " keyWord," +
                " uv," +
                " row_number() over(partition by date order by uv desc) rank " +
                " from daily_keyword_uv ) tmp " +
                " where rank <=3");

        //将DataFrame转换为RDD，然后映射，计算出每天的top3搜索词的搜索uv总数
        //格式：（日期，搜索词_uv）
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dateKeyWordTop3UvDS.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),row.getString(1)+"_"+row.getLong(2));
            }
        });
        JavaPairRDD<String, Iterable<String>> top3DateKeywordsRDD = top3DateKeywordUvRDD.groupByKey();

        JavaPairRDD<Long, String> top3TotalUvDateKeyword = top3DateKeywordsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String date = tuple._1;
                String dateKeywords = date;
                Long totalUv = 0L;
                Iterator<String> keywordUv = tuple._2.iterator();
                while (keywordUv.hasNext()) {
                    String keywordUvStr = keywordUv.next();
                    String[] splits = keywordUvStr.split("_");
                    dateKeywords += "," + keywordUvStr;
                    totalUv += Long.valueOf(splits[1]);
                }

                return new Tuple2<>(totalUv, dateKeywords);
            }
        });
        //按照每天的总搜索uv数进行倒叙排序
        JavaPairRDD<Long, String> sortedTop3TotalUvDateKeywordRDD = top3TotalUvDateKeyword.sortByKey(false);

        //
        JavaRDD<Row> sortedRowRDD = sortedTop3TotalUvDateKeywordRDD.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            @Override
            public Iterator<Row> call(Tuple2<Long, String> tuple) throws Exception {
                Long titalUv = tuple._1;
                String dateKeywordUv = tuple._2;
                String[] splits = dateKeywordUv.split(",");
                String date = splits[0];
                //第一个搜索词
                String[] keywordAndUvSplits = splits[1].split("_");
                String keyword = keywordAndUvSplits[0];
                String uv = keywordAndUvSplits[1];
                List<Row> list = new ArrayList<>();
                list.add(RowFactory.create(date, keyword, Long.valueOf(uv)));
                //第二个搜索词
                list.add(RowFactory.create(date, splits[2].split("_")[0], Long.valueOf(splits[2].split("_")[1])));
                //第三个搜索词
                list.add(RowFactory.create(date, splits[3].split("_")[0], Long.valueOf(splits[3].split("_")[1])));
                return list.iterator();
            }
        });

        Dataset<Row> resultDs = spark.createDataFrame(sortedRowRDD, structType);
        resultDs.createTempView("daily_top3_keyword_uv_table");
        spark.sql("select date,keyWord,uv from daily_top3_keyword_uv_table").show();

//        resultDs.write().saveAsTable("daily_top3_keyword_uv");


        spark.stop();
    }
}
