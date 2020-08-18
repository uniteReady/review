package com.tianya.bigdata.homework.day20200812;

import com.tianya.bigdata.homework.day20200801.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lionsoul.ip2region.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

public class ETLDriver02 extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static JobInfos jobInfos = null;

    /**
     * 有2个参数，输入路径和输出路径
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        if (args.length != 4) {
            LOGGER.error("输入参数不正确，需要两个参数：input output，当前参数个数为:" + args.length);
            for (int i = 0; i < args.length; i++) {
                LOGGER.error("第" + i + "个参数为:" + args[i]);
            }
            System.exit(0);
        }
        String input = args[1];
        String output = args[2];
        String day = args[3];
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
//        boolean flag = job.waitForCompletion(true);
        CounterGroup etlCounterGroup = job.getCounters().getGroup("etl");
        Iterator<Counter> iterator = etlCounterGroup.iterator();
        jobInfos = new JobInfos();
        while (iterator.hasNext()) {
            Counter counter = iterator.next();
            System.out.println(counter.getName() + "==>" + counter.getValue());
            switch (counter.getName()){
                case "access_totals":
                    jobInfos.setAccessTotals(Integer.valueOf(counter.getValue()+""));
                    break;
                case "access_formats":
                    jobInfos.setAccessFormats(Integer.valueOf(counter.getValue()+""));
                    break;
                case "access_error":
                    jobInfos.setAccessErrors(Integer.valueOf(counter.getValue()+""));
                    break;
                default:
                    break;
            }
        }
        long end = System.currentTimeMillis();
        jobInfos.setRunTime(Integer.valueOf((end-start)+""));
        jobInfos.setDay(day);
        jobInfos.setTaskName("access日志");

        conf.set("job_infos",jobInfos.toString());

        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop01:3306/ruozedata",
                "root",
                "root");

        //获取job2
        Job job2 = Job.getInstance(conf);

        //设置主类
        job2.setJarByClass(ETLDriver02.class);

        //设置map和reduce的类
        job2.setMapperClass(MyMapper2.class);

        //设置map和reduce的输出的key value类型
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(JobInfos.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job2,output);
        DBOutputFormat.setOutput(job2,"job_infos","task_name","totals","formats","errors","run_times","run_day");

        JobControl jobControl = new JobControl("MyGroup");
        ControlledJob cjob1 = new ControlledJob(job.getConfiguration());
        ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
        cjob2.addDependingJob(cjob1);

        jobControl.addJob(cjob1);
        jobControl.addJob(cjob2);

        new Thread(jobControl).start();
        while(! jobControl.allFinished()){
            Thread.sleep(100);
        }
        jobControl.stop();

        return 0;
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text,NullWritable,JobInfos>{

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String job_infos = context.getConfiguration().get("job_infos");
            String[] splits = job_infos.split("\t");
            JobInfos jobInfos = new JobInfos();
            jobInfos.setTaskName(splits[0]);
            jobInfos.setAccessTotals(Integer.valueOf(splits[1]));
            jobInfos.setAccessFormats(Integer.valueOf(splits[2]));
            jobInfos.setAccessErrors(Integer.valueOf(splits[3]));
            jobInfos.setRunTime(Integer.valueOf(splits[4]));
            jobInfos.setDay(splits[5]);
            context.write(NullWritable.get(),jobInfos);
        }
    }


    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        public SimpleDateFormat FORMAT = null;

        DbSearcher searcher = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FORMAT = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
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
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("etl", "access_totals").increment(1L);
            try {
//                Access access = LogParser.parseLog(value.toString());
                Access access = new Access();
                String[] splits = value.toString().split("\t");
                String time = splits[0];
                time = time.substring(1, time.length() - 1);
                Date date = FORMAT.parse(time);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                String ip = splits[1];
                String proxyIp = splits[2];
                String responseTime = splits[3];
                String referer = splits[4];
                String method = splits[5];
                String url = splits[6];
                String httpCode = splits[7];
                String requestSize = splits[8];
                String responseSize = splits[9];
                String cache = splits[10];
                String uaHead = splits[11];
                String type = splits[12];
                access.setIp(ip);
                access.setProxyIp(proxyIp);
                access.setResponseTime(Long.parseLong(responseTime));
                access.setReferer(referer);
                access.setMethod(method);
                access.setUrl(url);
                access.setHttpCode(httpCode);
                access.setRequestSize(Long.parseLong(requestSize));
                access.setCache(cache);
                access.setUaHead(uaHead);
                access.setType(type);

                Integer year = calendar.get(Calendar.YEAR);
                Integer month = calendar.get(Calendar.MONTH) + 1;
                Integer day = calendar.get(Calendar.DATE);
                access.setYear(year + "");
                access.setMonth(month < 10 ? "0" + month : month + "");
                access.setDay(day < 10 ? "0" + day : day + "");

                String ipInfo = IPUtils.getCityInfo(searcher, ip);
                String[] ipInfos = ipInfo.split("\\|");

                String province = ipInfos[2];
                String city = ipInfos[3];
                String isp = ipInfos[4];
                access.setProvince(province);
                access.setCity(city);
                access.setIsp(isp);

                String[] urlSplits = url.split("\\?");
                String[] urlSplits2 = urlSplits[0].split(":");

                String http = urlSplits2[0];
                access.setHttp(http);
                String urlSpliting = urlSplits2[1].substring(2);
                String domain = urlSpliting;
                String path = "";
                if (urlSpliting.contains("/")) {
                    domain = urlSpliting.substring(0, urlSpliting.indexOf("/"));
                    path = urlSpliting.substring(urlSpliting.indexOf("/"));
                }
                access.setDomain(domain);

                access.setPath(path);
                String params = urlSplits.length == 2 ? urlSplits[1] : null;
                access.setParams(params);
                access.setResponseSize(Long.parseLong(responseSize));
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
        /*Connection conn =null;
        PreparedStatement statement = null;
        try{
            conn = JDBCUtils.getConnection();
            String sql = "insert into job_infos(totals,formats,errors,run_times) values(?,?,?,?)";
            statement = conn.prepareStatement(sql);
            if(null != jobInfos){
                statement.setInt(1,jobInfos.getAccessTotals());
                statement.setInt(2,jobInfos.getAccessFormats());
                statement.setInt(3,jobInfos.getAccessErrors());
                statement.setInt(4,jobInfos.getRunTime());
                statement.execute();
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            JDBCUtils.close(conn,statement);
        }*/
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
