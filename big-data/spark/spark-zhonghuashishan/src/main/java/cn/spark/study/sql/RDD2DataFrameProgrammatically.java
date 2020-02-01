package cn.spark.study.sql;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameProgrammatically {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("RDD2DataFrameProgrammatically").master("local").getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.txt";

        //创建一个普通的RDD 
        JavaRDD<String> linesRDD = sc.textFile(path);
        //将普通RDD转换为RDD<Row>这种格式
        JavaRDD<Row> rows = linesRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] splits = line.split(",");
                return RowFactory.create(Integer.valueOf(splits[0]), splits[1], Integer.valueOf(splits[2]));
            }
        });
        //动态构造元数据
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = spark.createDataFrame(rows, structType);

        dataFrame.registerTempTable("students");

        Dataset<Row> teenagerDS
                = spark.sql("select * from students where age >=18");

        List<Row> rowList = teenagerDS.javaRDD().collect();

        for (Row row : rowList) {
            System.out.println(row);
        }


        spark.stop();


    }
}
