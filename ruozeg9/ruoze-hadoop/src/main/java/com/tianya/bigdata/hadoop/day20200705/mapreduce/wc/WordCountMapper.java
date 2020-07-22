package com.tianya.bigdata.hadoop.day20200705.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * wordcount 的mapper
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");

        for (String word : splits) {
            context.write(new Text(word),one);
        }
    }
}
