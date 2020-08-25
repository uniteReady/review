package com.tianya.bigdata.homework.day20200812;

import com.tianya.bigdata.homework.day20200801.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lionsoul.ip2region.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Locale;

/**
 * 处理访问日志数据
 * 需要配置环境变量
 * export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HIVE_HOME/lib/*:/home/hadoop/lib/*.jar:/home/hadoop/lib/mysql-connector-java-5.1.47.jar
 * export LIBJARS=/home/hadoop/lib/ip2region-1.7.2.jar
 *
 * 要用到的ip2region.db需要放到HDFS路径上
 *
 *
 */
public class ETLDriver02 extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    /**
     * 有3个参数，输入路径和输出路径
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        String startTime = sdf.format(sdf.parse(sdf.format(start)));
        if (args.length != 3) {
            LOGGER.error("输入参数不正确，需要三个参数：input output day，当前参数个数为:" + args.length);
            for (int i = 0; i < args.length; i++) {
                LOGGER.error("第" + i + "个参数为:" + args[i]);
            }
            System.exit(0);
        }
        String input = args[0];
        String output = args[1];
        String day = args[2];
        Configuration conf = super.getConf();
        FileUtils.delete(conf, output);
        Job job = Job.getInstance(conf);
        //设置主类
        job.setJarByClass(ETLDriver02.class);
        //设置输入输出路径
        Path src = new Path(input);
        Path dst = new Path(output);
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
        JobInfos jobInfos = new JobInfos();
        while (iterator.hasNext()) {
            Counter counter = iterator.next();
            System.out.println(counter.getName() + "==>" + counter.getValue());
            switch (counter.getName()) {
                case "access_totals":
                    jobInfos.setAccessTotals(Integer.valueOf(counter.getValue() + ""));
                    break;
                case "access_formats":
                    jobInfos.setAccessFormats(Integer.valueOf(counter.getValue() + ""));
                    break;
                case "access_error":
                    jobInfos.setAccessErrors(Integer.valueOf(counter.getValue() + ""));
                    break;
                default:
                    break;
            }
        }
        long end = System.currentTimeMillis();
        String endTime = sdf.format(sdf.parse(sdf.format(end)));
        //毫秒值
        jobInfos.setRunTime(Integer.valueOf((end - start) + ""));
        jobInfos.setDay(day);
        jobInfos.setTaskName("access日志");
        jobInfos.setStartTime(startTime);
        jobInfos.setEndTime(endTime);

        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = JDBCUtils.getConnection();
            String sql = "insert into job_infos(task_name,totals,formats,`errors`,run_times,`day`,start_time,end_time) values(?,?,?,?,?,?,?,?)";
            statement = conn.prepareStatement(sql);
            if (null != jobInfos) {
                statement.setString(1, jobInfos.getTaskName());
                statement.setInt(2, jobInfos.getAccessTotals());
                statement.setInt(3, jobInfos.getAccessFormats());
                statement.setInt(4, jobInfos.getAccessErrors());
                statement.setInt(5, jobInfos.getRunTime());
                statement.setString(6, jobInfos.getDay());
                statement.setString(7, jobInfos.getStartTime());
                statement.setString(8, jobInfos.getEndTime());
                statement.executeUpdate();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            JDBCUtils.close(conn, statement);
        }

        return 0;
    }


    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        public int bufferSize = 10240;
        public SimpleDateFormat FORMAT = null;

        DbSearcher searcher = null;
        FSDataInputStream fsDataInputStream = null;
        ByteArrayOutputStream byteArrayOutputStream = null;

        /*@Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
            //读取Linux本地上的ip2region.db文件
            *//*FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            String dbPath = "/ruozedata/dw/data/ip2region.db";
            fsDataInputStream = fileSystem.open(new Path(dbPath), 2048);*//*
            String dbLocalPath = "/home/hadoop/app/ruozedata-dw/data/ip2region.db";
            FileInputStream filePartIn0 = new FileInputStream(new File(dbLocalPath));
            byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(filePartIn0, byteArrayOutputStream, bufferSize);
            byte[] bytes = byteArrayOutputStream.toByteArray();
            DbConfig config = null;
            try {
                config = new DbConfig();
                searcher = new DbSearcher(config, bytes);
            } catch (DbMakerConfigException e) {
                e.printStackTrace();
            }
        }*/

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
            //读取HDFS上的ip2region.db文件
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            String dbPath = "/home/hadoop/app/ruozedata-dw/data/ip2region.db";
            /*fsDataInputStream = fileSystem.open(new Path(dbPath), bufferSize);
            byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(fsDataInputStream, byteArrayOutputStream, bufferSize);
            byte[] bytes = byteArrayOutputStream.toByteArray();*/
            DbConfig config = null;
            try {
                config = new DbConfig();
                searcher = new DbSearcher(config, dbPath);
            } catch (DbMakerConfigException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            IOUtils.closeStream(fsDataInputStream);
            IOUtils.closeStream(byteArrayOutputStream);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("etl", "access_totals").increment(1L);
            try {
                Access access = LogParser.parseLog(searcher, value.toString());
                context.getCounter("etl", "access_formats").increment(1L);
                context.write(new Text(access.toString()), NullWritable.get());
            } catch (Exception e) {
                context.getCounter("etl", "access_error").increment(1L);
                System.out.println("错误日志" + value.toString());
            }

        }
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new ETLDriver02(), args);
        System.exit(result);


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
//        String dbPath = ETLDriver02.class.getResource("/home/hadoop/app/ruozedata-dw/data/ip2region.db").getPath();
        String dbPath = "/home/hadoop/app/ruozedata-dw/data/ip2region.db";


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
