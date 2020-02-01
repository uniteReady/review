package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameCreate {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("DataFrameCreate").getOrCreate();

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.json";
        Dataset<Row> jsonDS = spark.read().json(path);

        jsonDS.show();


        spark.stop();
    }
}
