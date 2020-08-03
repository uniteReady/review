package com.tianya.bigdata.homework.day20200801;

import java.io.File;
import java.lang.reflect.Method;

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
import org.lionsoul.ip2region.DataBlock;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;
import org.lionsoul.ip2region.Util;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringJoiner;

public class ETLDriver {

    public static class MyMapper extends Mapper<LongWritable,Text, Text, NullWritable>{

        public static final SimpleDateFormat FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
        public static final SimpleDateFormat FORMAT2 = new SimpleDateFormat("yyyyMMdd");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringJoiner sj = new StringJoiner("|");
            String accessLog = value.toString();
            String[] splits = accessLog.split(" ");
            try {
                //清洗时间 转换成yyyyMMdd
                String time = splits[0]+" "+ splits[1];
                time = time.substring(1,time.length()-1);
                Date date = FORMAT.parse(time);
                String day = FORMAT2.format(date);
                String ip = splits[2];
                String ipInfo = analizeIp(ip);
                String responseSize = splits[10];
                if(responseSize == null || !isNumber(responseSize)){
                    responseSize = null;
                }
                String agentIp = splits[3];
                String responseTime = splits[4];
                String referer = splits[5];
                String method = splits[6].substring(1);
                String url = splits[7].substring(0,splits[7].length()-1);
                String httpCode = splits[8];
                String requestSize = splits[9];
                String cacheStatus = splits[11];
                String UAHead = splits[12].substring(1) + " " + splits[13] + " " + splits[14].substring(0,splits[14].length()-1);
                String type = splits[15].substring(1,splits[15].length() - 1);
                if(null != responseSize){
                    sj.add(time)
                            .add(ip)
                            .add(ipInfo)
                            .add(agentIp)
                            .add(responseTime)
                            .add(referer)
                            .add(method)
                            .add(url)
                            .add(httpCode)
                            .add(requestSize)
                            .add(responseSize)
                            .add(cacheStatus)
                            .add(UAHead)
                            .add(type)
                            .add(day);
                    context.write(new Text(sj.toString()),NullWritable.get());
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }

    public static class MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        String out = "hdfs://hadoop01:9000/G90401/etl_temp";
        FileUtils.delete(conf,out);

        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(ETLDriver.class);

        //设置输入输出路径
        Path src = new Path("hdfs://hadoop01:9000/G90401/data");
//        Path src = new Path(args[0]);
        Path dst = new Path(out);
        FileInputFormat.setInputPaths(job,src);
        FileOutputFormat.setOutputPath(job,dst);


        //设置Map的K V 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置Map 的类

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);


        boolean flag = job.waitForCompletion(true);

        System.exit(flag?1:0);


    }


    /**
     * 判断一个字符串是否为数字类型
     * @param str
     * @return
     */
    private static boolean isNumber(String str){
        String reg = "^[0-9]*$";
        return str.matches(reg);
    }


    public static String analizeIp(String ip){
        //db
        String dbPath = ETLDriver.class.getResource("/ip2region.db").getPath();

        File file = new File(dbPath);
        if ( file.exists() == false ) {
            System.out.println("Error: Invalid ip2region.db file");
        }

        //查询算法
        int algorithm = DbSearcher.BTREE_ALGORITHM; //B-tree
        //DbSearcher.BINARY_ALGORITHM //Binary
        //DbSearcher.MEMORY_ALGORITYM //Memory
        try {
            DbConfig config = new DbConfig();
            DbSearcher searcher = new DbSearcher(config, dbPath);

            //define the method
            Method method = null;
            switch ( algorithm )
            {
                case DbSearcher.BTREE_ALGORITHM:
                    method = searcher.getClass().getMethod("btreeSearch", String.class);
                    break;
                case DbSearcher.BINARY_ALGORITHM:
                    method = searcher.getClass().getMethod("binarySearch", String.class);
                    break;
                case DbSearcher.MEMORY_ALGORITYM:
                    method = searcher.getClass().getMethod("memorySearch", String.class);
                    break;
            }

            DataBlock dataBlock = null;
            if ( Util.isIpAddress(ip) == false ) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock  = (DataBlock) method.invoke(searcher, ip);

            return dataBlock.getRegion();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }




}
