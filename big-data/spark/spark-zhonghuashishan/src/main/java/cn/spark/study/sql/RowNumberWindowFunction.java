package cn.spark.study.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * row_number()开窗函数实战
 */
public class RowNumberWindowFunction {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("RowNumberWindowFunction").master("local").getOrCreate();

        //创建销售额表，sales表
        spark.sql("DROP TABLE IF EXISTS sales");
        spark.sql("CREATE TABLE IF NOT EXISTS sales(product string,category string,revenue bigint)");
        spark.sql("load data local inpath '//application/tianyafu/sales.txt' into table sales");

        //开始编写我们的统计逻辑，使用row_number()开窗函数
        /**
         * row_number()开窗函数的作用，其实就是给每个分组的数据，按照其顺序排序，打上一个分组内的行号
         * 比如说：有一个分组date=20151001,里面有3条数据，1122,1121,1124，那么对这个分组的每一行使用row_number()开窗函数以后，
         * 三行，依次会获得一个组内的行号，行号从1开始递增，比如1121 1,1122 2,1124 3
         *
         *  //row_number()开窗函数的语法说明，首先可以在select查询时，使用row_number()函数，
         *  // 其次，row_number()函数后面先跟上OVER关键字，然后括号中，是PARTITION BY,也就是说根据
         *  //那个字段进行分组，其次是可以使用order by 进行组内排序，然后row_number()就可以给每个组内的行，一个组内行号
         */
        Dataset<Row> top3SaleDF = spark.sql("select " +
                "product,category,revenue " +
                "from (" +
                "select " +
                " product," +
                " category," +
                " revenue," +
                " row_number() OVER(PARTITION BY category order by revenue desc) rank " +
                " from " +
                " sales ) " +
                "where rank <=3  ");

        spark.sql("DROP TABLE IF EXISTS top3_sales");
        top3SaleDF.write().saveAsTable("top3_sales");

        spark.stop();
    }
}
