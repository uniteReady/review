package com.tianya.bigdata.hadoop.day20200712.sort;

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

/**
 * 全局排序
 */
public class AllSortDriver {

    public static void main(String[] args) throws Exception {
        String in = "ruozeg9/ruozedata-hadoop/data/access.log";
        String out = "out";

        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //如果输出目录存在，则删除
        FileUtils.delete(conf,out);

        //设置map 和 reduce的输出key value
        job.setMapOutputKeyClass(Traffic.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Traffic.class);

        //设置输入输出类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置主类
        job.setJarByClass(AllSortDriver.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,new Path(out));

        //执行
        boolean success = job.waitForCompletion(true);

        //退出
        System.exit(success?0:1);

    }

    public static class MyMapper extends Mapper<LongWritable,Text, Traffic,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String phone = splits[1];
            long up = Long.valueOf(splits[splits.length -3]);
            long down = Long.valueOf(splits[splits.length -2]);
            context.write(new Traffic(up,down),new Text(phone));
        }
    }

    public static class MyReducer extends Reducer<Traffic,Text, Text, Traffic>{
        @Override
        protected void reduce(Traffic traffic, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text phone : values) {
                context.write(phone,traffic);
            }
        }
    }
}
