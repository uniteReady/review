package com.tianya.bigdata.homework.day20200812;

import com.tianya.bigdata.homework.day20200801.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.lionsoul.ip2region.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Locale;

public class ETLDriver01 {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        public SimpleDateFormat FORMAT = null;
        DbSearcher searcher = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
            //读取HDFS上的ip2region.db文件
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            String dbPath = "/ruozedata/dw/data/ip2region.db";
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(dbPath), 2048);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(fsDataInputStream,byteArrayOutputStream,2048);
            byte[] bytes = byteArrayOutputStream.toByteArray();
            DbConfig config = null;
            try {
                config = new DbConfig();
                searcher = new DbSearcher(config, bytes);
            } catch (DbMakerConfigException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("etl", "access_totals").increment(1L);
            try {
                Access access = LogParser.parseLog(searcher,value.toString());
                if (null != access) {
                    context.write(new Text(access.toString()), NullWritable.get());
                    context.getCounter("etl", "access_formats").increment(1L);
                } else {
                    context.getCounter("etl", "access_error").increment(1L);
                }
            } catch (Exception e) {
                LOGGER.error(value.toString());
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        String in = "ruozeg9/ruoze-hadoop/data/access.log";
        String out = "out/ods";
        FileUtils.delete(conf, out);

        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(ETLDriver01.class);

        //设置输入输出路径
        Path src = new Path(in);
//        Path src = new Path(args[0]);
        Path dst = new Path(out);
        FileInputFormat.setInputPaths(job, src);
        FileOutputFormat.setOutputPath(job, dst);


        //设置Map的K V 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置Map 的类
        job.setMapperClass(MyMapper.class);

        boolean flag = job.waitForCompletion(true);
        CounterGroup etlCounterGroup = job.getCounters().getGroup("etl");
        Iterator<Counter> iterator = etlCounterGroup.iterator();
        while (iterator.hasNext()) {
            Counter counter = iterator.next();
            System.out.println(counter.getName() + "==>" + counter.getValue());
        }

        System.exit(flag ? 1 : 0);


    }


    /**
     * 判断一个字符串是否为数字类型
     *
     * @param str
     * @return
     */
    private static boolean isNumber(String str) {
        String reg = "^[0-9]*$";
        return str.matches(reg);
    }


    public static String analizeIp(String ip) {
        //db
        String dbPath = ETLDriver01.class.getResource("/ip2region.db").getPath();

        File file = new File(dbPath);
        if (file.exists() == false) {
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
            switch (algorithm) {
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
            if (Util.isIpAddress(ip) == false) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock = (DataBlock) method.invoke(searcher, ip);

            return dataBlock.getRegion();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


}
