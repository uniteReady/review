package com.tianya.bigdata.hadoop.day20200711;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import com.tianya.bigdata.hadoop.day20200708.Access;
import com.tianya.bigdata.hadoop.day20200708.AccessDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionerDriver {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Access> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String phone = splits[1];
            Long up = Long.valueOf(splits[splits.length - 3]);
            Long down = Long.valueOf(splits[splits.length - 2]);
            context.write(new Text(phone),new Access(phone,up,down));
        }
    }


    public static class MyReducer extends Reducer<Text, Access, NullWritable, Access> {
        @Override
        protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
            Long ups = 0L;
            Long downs = 0L;
            for (Access value : values) {
                ups += value.getUp();
                downs += value.getDown();
            }
            context.write(NullWritable.get(),new Access(key.toString(),ups,downs));
        }
    }

    public static void main(String[] args) throws Exception {

        String in = "ruozeg9/ruozedata-hadoop/data/access.log";
        String out = "out";
        //创建一个Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileUtils.delete(conf,out);

        //设置主类
        job.setJarByClass(AccessDriver.class);

        //设置分区器
        job.setPartitionerClass(PhonePartitioner.class);
        //设置分区器后要设置reduce的个数
        job.setNumReduceTasks(3);

        //设置Combiner,Combiner是map端的reduce，所以Combiner的类就是reduce的类，当然也可以自己再另外实现一套逻辑
        job.setCombinerClass(MyReducer.class);

        //设置mapper和reduce的类
        job.setMapperClass(AccessDriver.MyMapper.class);
        job.setReducerClass(AccessDriver.MyReducer.class);

        //设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        //设置输入输出的路径

        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,new Path(out));

        //提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
