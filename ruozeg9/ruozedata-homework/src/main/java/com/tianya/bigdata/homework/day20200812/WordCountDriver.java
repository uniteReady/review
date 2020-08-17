package com.tianya.bigdata.homework.day20200812;

import com.tianya.bigdata.homework.day20200801.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbMakerConfigException;
import org.lionsoul.ip2region.DbSearcher;

import java.io.IOException;

public class WordCountDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        String in = "/ruozedata/dw/raw/access/20190101/access.log";
        String out = "/ruozedata/wordcount";

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);

        FileUtils.delete(conf,out);

        //设置map 和 reducer的类

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置主类
        job.setJarByClass(WordCountDriver.class);

        //设置map 和 reducer的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置job的输入输出
        FileInputFormat.setInputPaths(job,new Path(in));
        FileOutputFormat.setOutputPath(job,new Path(out));

        //等待完成
        boolean flag = job.waitForCompletion(true);

        return flag?0:1;
    }

    public static class MyMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        DbSearcher searcher=null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String dbPath = "/home/hadoop/app/ruozedata-dw/data/ip2region.db";
            DbConfig config = null;
            try {
                config = new DbConfig();
                searcher = new DbSearcher(config, dbPath);
            } catch (DbMakerConfigException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String cityInfo = IPUtils.getCityInfo(searcher,splits[1]);
            if(null == cityInfo){
                Text nullText = new Text("是空的");
                context.write(nullText, NullWritable.get());

            }else{
                Text city = new Text(cityInfo);
                context.write(city,NullWritable.get());
            }
        }
    }

    public static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        int result = ToolRunner.run(conf, new WordCountDriver(), args);
        System.exit(result);


    }
}
