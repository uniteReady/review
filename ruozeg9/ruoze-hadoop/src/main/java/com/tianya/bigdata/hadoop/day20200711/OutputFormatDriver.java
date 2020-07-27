package com.tianya.bigdata.hadoop.day20200711;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义OutputFormat
 */
public class OutputFormatDriver {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<Text,NullWritable, Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key,NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception{
        String in = "ruozeg9/ruozedata-hadoop/data/hostname.log";
        String out = "out";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileUtils.delete(conf,out);

        //设置MAP和REDUCE输出的key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置主类
        job.setJarByClass(OutputFormatDriver.class);

        //设置输入输出类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,in);
        job.setOutputFormatClass(MyOutputFormat.class);
        MyOutputFormat.setOutputPath(job,new Path(out));

        //开始执行
        boolean success = job.waitForCompletion(true);
        System.exit(success?0:1);
    }

    public static class MyOutputFormat extends FileOutputFormat<Text,NullWritable>{

        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new MyRecordWriter(job);
        }
    }

    public static class MyRecordWriter extends RecordWriter<Text, NullWritable> {
        FileSystem fileSystem = null;
        FSDataOutputStream ruozeStream = null;
        FSDataOutputStream otherStream = null;


        public MyRecordWriter(TaskAttemptContext job){
            try {
                fileSystem = FileSystem.get(job.getConfiguration());
                ruozeStream = fileSystem.create(new Path("out/ruoze/"));
                otherStream = fileSystem.create(new Path("out/other/"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Writes a key/value pair.
         *
         * @param key   the key to write.
         * @param value the value to write.
         * @throws IOException
         */
        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if(key.toString().contains("ruoze")){
                ruozeStream.write((key.toString()+"\r\n").getBytes());
            }else{
                otherStream.write((key.toString()+"\r\n").getBytes());
            }

        }

        /**
         * Close this <code>RecordWriter</code> to future operations.
         *
         * @param context the context of the task
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(ruozeStream);
            IOUtils.closeStream(otherStream);
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }
}
