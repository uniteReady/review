package cn.spark.study.sql;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ManuallySpecifyOptions {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("ManuallySpecifyOptions").getOrCreate();

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.json";
        //读取json
        Dataset<Row> json = spark.read().format("json").load(path);
        Dataset<Row> json1 = spark.read().json(path);
        json.show();
        json1.show();

        //保存数据
        //json.select("name").write().format("parquet").save("E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\parquets");


        String path2 = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\spark-data\\users.parquet";

        Dataset<Row> users = spark.read().format("parquet").load(path2);
        users.show();
        users.registerTempTable("users");
        Dataset<Row> nameDS = spark.sql("select name from users");

        List<String> names = nameDS.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return "Name: " + v1.getString(0);
            }
        }).collect();

        for (String name : names) {
            System.out.println(name);
        }


        spark.stop();
    }
}
