package cn.spark.study.project.spark;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        //获取sparkSession
        SparkSession spark= getSparkSession();
        //获取JavaSparkContext
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //关闭spark上下文
        spark.stop();

    }

    public static SparkSession getSparkSession(){
        SparkSession.Builder builder = SparkSession.builder().appName(ConfigurationManager.getProperty(Constants.SPARK_APP_NAME_SESSION)).master("local");
        //如果不是本地模式  我们让他开启hive支持
        if (!ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            builder = builder.enableHiveSupport();
        }
        return builder.getOrCreate();

    }


}
