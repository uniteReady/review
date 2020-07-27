package com.tianya.bigdata.hadoop.day20200711;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyMySQLDBInputFormatDriver {

    public static class MyMapper extends Mapper<LongWritable, DeptWritable, NullWritable, DeptWritable>{
        @Override
        protected void map(LongWritable key, DeptWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }

    public static void main(String[] args) throws Exception {

        String out = args[0];
        //创建一个Job
        Configuration conf = new Configuration();
        String driverClass = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.101.217:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8";
        String userName = "dev";
        String passwd = "lJZx2Ik5eqX3xBDp";
        DBConfiguration.configureDB(conf,driverClass,dbUrl,userName,passwd);

        Job job = Job.getInstance(conf);

        job.setInputFormatClass(DBInputFormat.class);


        FileUtils.delete(conf,out);

        //设置主类
        job.setJarByClass(MyMySQLDBInputFormatDriver.class);

        //设置mapper和reduce的类
        job.setMapperClass(MyMapper.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeptWritable.class);

        //设置reduce的输出类型
        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);*/

        //设置输入输出的路径
        String[] fields = new String[]{"DEPTNO","DNAME","LOC"};
        DBInputFormat.setInput(job, DeptWritable.class,"dept",null,null,fields);
        FileOutputFormat.setOutputPath(job,new Path(out));

        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
