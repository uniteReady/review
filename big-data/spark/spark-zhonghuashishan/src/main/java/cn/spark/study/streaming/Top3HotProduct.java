package cn.spark.study.streaming;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 与spark sql整合起来使用，top3热门商品
 */
public class Top3HotProduct {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("Top3HotProduct").master("local[2]").getOrCreate();
        JavaStreamingContext jssc = new JavaStreamingContext(spark.sparkContext().getConf(), Durations.seconds(10));

        //日志格式：name product category
        JavaReceiverInputDStream<String> lineDstream = jssc.socketTextStream("master", 9999);
        JavaPairDStream<String, Integer> pairDStream = lineDstream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] splits = line.split(" ");
                return new Tuple2<>(splits[2] + "_" + splits[1], 1);
            }
        });
        JavaPairDStream<String, Integer> reduceByKeyAndWindowDstream = pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));
        //Row(category,product,count)
        JavaDStream<Row> rowJavaDStream = reduceByKeyAndWindowDstream.map(new Function<Tuple2<String, Integer>, Row>() {
            @Override
            public Row call(Tuple2<String, Integer> tuple) throws Exception {
                return RowFactory.create(tuple._1.split("_")[0], tuple._1.split("_")[0], tuple._2);
            }
        });

        rowJavaDStream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {
                StructType structType = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("category", DataTypes.StringType, true),
                        DataTypes.createStructField("product", DataTypes.StringType, true),
                        DataTypes.createStructField("click_num", DataTypes.IntegerType, true)
                ));
                Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, structType);
                dataFrame.createTempView("product_click_log");
                Dataset<Row> resultDs = spark.sql("select " +
                        "category,product,click_num " +
                        "from (" +
                        "select " +
                        "category," +
                        "product," +
                        "click_num," +
                        "row_number() over (partition by category order by click_num desc) rank " +
                        "from product_click_log ) tmp " +
                        "where rank <=3");
                resultDs.show();
            }
        });

        jssc.start();
        jssc.awaitTermination();

        spark.stop();
    }
}
