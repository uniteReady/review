package cn.spark.study.sql;

import org.apache.spark.sql.*;

public class DataFrameOperation {

    public static void main(String[] args) throws  Exception {
        SparkSession spark = SparkSession.builder().appName("DataFrameOperation").master("local").getOrCreate();

        String path = "E:\\WorkSpace\\IDEAWorkspace\\review\\big-data\\spark\\spark-zhonghuashishan\\src\\main\\resources\\students.json";
        //源码： type DataFrame = Dataset[Row]
        Dataset<Row> jsonDS = spark.read().json(path);
        //默认打印Dataset中所有数据的前20行
        jsonDS.show();
        //打印出Dataset的元数据
        jsonDS.printSchema();
        /**
         * DSL风格的api
         */
        //查询出某列的前20行数据
        jsonDS.select("name").show();
        //查询某几列的前20行数据，并对列进行计算
        jsonDS.select(jsonDS.col("name"), jsonDS.col("age").plus(1)).show();
        //根据某一列的值进行过滤
        jsonDS.filter(jsonDS.col("age").gt(18)).show();
        //根据某一列进行分组，然后进行聚合
        jsonDS.groupBy(jsonDS.col("age")).count().show();

        Encoder<Student> studentBean = Encoders.bean(Student.class);
        Dataset<Row> dataFrame = spark.createDataFrame(jsonDS.javaRDD(), Student.class);
        /**
         * SQL风格的api
         */
        //注册成临时表
        jsonDS.createTempView("students");
        Dataset<Row> man = spark.sql("select * from students where age >18");

        //转换为DS<T>
        Dataset<Student> as = dataFrame.as(studentBean);
        Dataset<Student> studentDataset = jsonDS.as(studentBean);
        studentDataset.show();
        //注册成临时表
//        studentDataset.createTempView("student2");
//        Dataset<Row> man2 = spark.sql("select * from student2 where age > 18");


        spark.stop();
    }
}
