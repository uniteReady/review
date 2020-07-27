package com.tianya.bigdata.hadoop.day20200711;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountCombinerDriver {

    public static class MyMapper extends Mapper<LongWritable, Text, Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            for (String word : words) {
                context.write(new Text(word),new LongWritable(1L));
            }
        }
    }

    public static class MyReducer extends Reducer<Text,LongWritable, Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception{
        String in = "ruozeg9/ruozedata-hadoop/data/ruozedata.txt";
        String out = "out";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileUtils.delete(conf,out);

        //设置MAP和REDUCE输出的key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置主类
        job.setJarByClass(WordCountCombinerDriver.class);

        //设置输入输出类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置Combiner类
        job.setCombinerClass(MyReducer.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,new Path(out));

        //开始执行
        boolean success = job.waitForCompletion(true);
        System.exit(success?0:1);


    }
}
