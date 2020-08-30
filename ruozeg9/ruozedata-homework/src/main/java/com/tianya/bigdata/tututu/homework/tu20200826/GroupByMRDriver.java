package com.tianya.bigdata.tututu.homework.tu20200826;


import com.tianya.bigdata.homework.day20200801.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * 当发生group by的数据倾斜时，mapreduce的实现就是利用2个job，先把key打散，再把key收敛
 * Hive的实现是通过参数调节：hive.map.aggr=true开启map端的combiner，hive.groupby.skewindata=true，该参数实现先打散后收敛的效果
 *
 *  create external table ruozedata.student_scores(
 *  subject_id int,
 *  subject string,
 *  score string
 *  ) comment '学生成绩表，大表' row format delimited fields terminated by '\t' location '/ruozedata/hive/student_scores' ;
 *
 * 要实现 select subject,avg(score)  from student_scores group by subject;的功能
 *
 *
 */
public class GroupByMRDriver {

    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Random random = new Random();
            String[] splits = value.toString().split("\t");
            String subject = splits[1];
            String score = splits[2];
            context.write(new Text(random.nextInt(10)+"_"+subject),new Text(score+"|"+1));
        }
    }

    public static class MyReducer1 extends Reducer<Text, Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double cnt = 0;
            for (Text value : values) {
                String[] splits = value.toString().split("\\|");
                sum += Double.valueOf(splits[0]);
                cnt += Double.valueOf(splits[1]);
            }
            context.write(key,new Text(sum+"|"+cnt));

        }
    }


    public static class MyMapper2 extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            context.write(new Text(splits[0].substring(splits[0].indexOf("_")+1)),new Text(splits[1]));
        }
    }

    public static class MyReducer2 extends Reducer<Text, Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double cnt = 0;
            for (Text value : values) {
                String[] splits = value.toString().split("\\|");
                sum += Double.valueOf(splits[0]);
                cnt += Double.valueOf(splits[1]);
            }
            String avg = (sum/cnt)+"";
            context.write(key,new Text(avg));

        }
    }



    public static void main(String[] args) throws Exception{
        String in = "out/studentScores.txt";
        String out1 = "out/job1_out";
        String out2 = "out/job2_out";
        //获取一个job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);

        FileUtils.delete(conf,out1);
        FileUtils.delete(conf,out2);

        //设置主类
        job1.setJarByClass(GroupByMRDriver.class);

        //设置map和reduce的类
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);

        //设置map和reduce的输出key value类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job1,in);
        FileOutputFormat.setOutputPath(job1,new Path(out1));


        //获取job2
        Job job2 = Job.getInstance(conf);

        //设置主类
        job2.setJarByClass(GroupByMRDriver.class);

        //设置map和reduce的类
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        //设置map和reduce的输出的key value类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

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
