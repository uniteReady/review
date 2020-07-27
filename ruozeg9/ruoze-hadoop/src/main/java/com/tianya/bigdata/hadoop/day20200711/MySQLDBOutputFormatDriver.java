package com.tianya.bigdata.hadoop.day20200711;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MySQLDBOutputFormatDriver {

    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MyReducer extends Reducer<LongWritable, Text, DeptWritable, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] splits = value.toString().split(",");
                DeptWritable deptWritable = new DeptWritable();
                deptWritable.setDeptno(Integer.valueOf(splits[0]));
                deptWritable.setDname(splits[1]);
                deptWritable.setLoc(splits[2]);
                context.write(deptWritable, NullWritable.get());
            }
        }
    }

    public static class MySQLDBOutputFormat extends DBOutputFormat<DeptWritable, NullWritable> {

    }


    public static void main(String[] args) throws Exception {
        String in = "ruozeg9/ruozedata-hadoop/data/dept.txt";

        Configuration conf = new Configuration();
        String driverClass = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.101.217:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8";
        String userName = "dev";
        String passwd = "lJZx2Ik5eqX3xBDp";
        DBConfiguration.configureDB(conf, driverClass, dbUrl, userName, passwd);

        Job job = Job.getInstance(conf);

        //设置输出的key value 类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DeptWritable.class);
        job.setOutputValueClass(NullWritable.class);

        //设置map和reduce的类

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置主类
        job.setJarByClass(MySQLDBOutputFormatDriver.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job, in);
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "dept", "DEPTNO", "DNAME", "LOC");

        //执行
        boolean success = job.waitForCompletion(true);
        //退出
        System.exit(success ? 0 : 1);
    }
}
