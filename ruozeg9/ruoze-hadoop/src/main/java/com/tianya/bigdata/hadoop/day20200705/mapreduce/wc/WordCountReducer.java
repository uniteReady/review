package com.tianya.bigdata.hadoop.day20200705.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        while(values.iterator().hasNext()){
            IntWritable value = values.iterator().next();
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
