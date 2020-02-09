package cn.spark.study.sql;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * http://spark.apache.org/docs/2.3.3/sql-programming-guide.html#jdbc-to-other-databases
 *
 * create table students_infos(name varchar(20),age int);
 *
 *
 * create table student_scores(name varchar(20),score int);
 *
 * insert into students_infos values('leo',18),('marry',17),('jack',19);
 *
 * insert into student_scores values ('leo',88),('marry',99),('jack',60);
 *
 * create table good_student_infos(name varchar(20),age int,score int);
 */
public class JDBCDataSource {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("JDBCDataSource").getOrCreate();
        Map<String,String > options = new HashMap<>();
        options.put("url","jdbc:mysql://localhost:3306/test");
        options.put("dbtable","students_infos");
        options.put("user","root");
        options.put("password","root");
        Dataset<Row> studentInfosDS = spark.read().format("jdbc").options(options).load();

        options.put("dbtable","student_scores");
        Dataset<Row> studentScoreDS = spark.read().format("jdbc").options(options).load();
        /**
         * 第一种方式 将得到的两个DS分别转换成pairRDD ，然后进行join
         * 第二种方式 将得到的两个DS分别转换为TempTable
         */
        /*
        //第一种方式
        JavaPairRDD<String, Integer> studentScorePairRDD = studentScoreDS.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        });
        JavaPairRDD<String, Integer> studentInfosPairRDD = studentInfosDS.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        });
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentInfoScorePairRDD = studentInfosPairRDD.<Integer>join(studentScorePairRDD);
        JavaRDD<Row> studentInfoScoreRowRDD = (JavaRDD<Row>) studentInfoScorePairRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2._1, v1._2._2);
            }
        });
        //过滤出分数大于80分的学生作为good student
        JavaRDD<Row> goodStudents = studentInfoScoreRowRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return row.getInt(2) > 80;
            }
        });

        //转换为DataSet
        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFieldList.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFieldList.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFieldList);
        Dataset<Row> resultDS = spark.createDataFrame(goodStudents, structType);

        //将DataSet中的数据保存到mysql中
        options.put("dbtable","good_student_infos");
        resultDS.write().format("jdbc").options(options).mode(SaveMode.Append).save();
        */

        //第二种方式
        studentInfosDS.registerTempTable("student_infos");
        studentScoreDS.registerTempTable("student_score");
        Dataset<Row> studentInfoScoreDS = spark.sql("select a.name,a.age,b.score from student_infos a left join student_score b on a.name = b.name where b.score >80");

        options.put("dbtable","good_student_infos");
        studentInfoScoreDS.write().format("jdbc").mode(SaveMode.Append).options(options).save();


        spark.stop();

    }
}
