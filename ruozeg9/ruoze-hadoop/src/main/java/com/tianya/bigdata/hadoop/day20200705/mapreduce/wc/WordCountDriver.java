package com.tianya.bigdata.hadoop.day20200705.mapreduce.wc;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws Exception {

        String in = "ruozeg9/ruozedata-hadoop/data/ruozedata.txt";
        String out = "out";
        //创建一个Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileUtils.delete(conf,out);

        //设置主类
        job.setJarByClass(WordCountDriver.class);

        //设置mapper和reduce的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出的路径

        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,new Path(out));

        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
