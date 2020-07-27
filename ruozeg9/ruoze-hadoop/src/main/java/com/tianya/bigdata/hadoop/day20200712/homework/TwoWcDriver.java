package com.tianya.bigdata.hadoop.day20200712.homework;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * 变形的wc
 * 先将word拼接一个随机数，进行wc，然后再将随机数去掉，再次wc
 * 该功能可以进行数据倾斜的优化
 * 利用JobController将2个job串联起来，并设置依赖
 */
public class TwoWcDriver {

    public static class MyMapper1 extends Mapper<LongWritable,Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Random random = new Random();
            String[] words = value.toString().split(",");
            for (String word : words) {
                context.write(new Text(random.nextInt(10)+"_"+word),new IntWritable(1));
            }
        }
    }

    public static class MyReducer1 extends Reducer<Text, IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }


    public static class MyMapper2 extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            context.write(new Text(splits[0].substring(splits[0].indexOf("_")+1)),new IntWritable(Integer.valueOf(splits[1])));
        }
    }



    public static void main(String[] args) throws Exception{
        String in = "ruozeg9/ruozedata-hadoop/data/ruozedata.txt";
        String out1 = "out/job1_out";
        String out2 = "out/job2_out";
        //获取一个job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        FileUtils.delete(conf,out1);
        FileUtils.delete(conf,out2);

        //设置主类
        job1.setJarByClass(TwoWcDriver.class);

        //设置map和reduce的类
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);

        //设置map和reduce的输出key value类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job1,in);
        FileOutputFormat.setOutputPath(job1,new Path(out1));


        //获取job2
        Job job2 = Job.getInstance(conf);

        //设置主类
        job2.setJarByClass(TwoWcDriver.class);

        //设置map和reduce的类
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer1.class);

        //设置map和reduce的输出的key value类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job2,out1);
        FileOutputFormat.setOutputPath(job2,new Path(out2));

        JobControl jobControl = new JobControl("MyGroup");
        ControlledJob cjob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
        cjob2.addDependingJob(cjob1);

        jobControl.addJob(cjob1);
        jobControl.addJob(cjob2);

        new Thread(jobControl).start();
        while(! jobControl.allFinished()){
            Thread.sleep(100);
        }
        jobControl.stop();


    }
}
