package com.tianya.bigdata.hadoop.day20200711;

import com.tianya.bigdata.hadoop.day20200705.mapreduce.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 第一种方法（不推荐）
 * 利用hadoop classpath找到classpath的目录，直接将mysql驱动包上传到相应的目录，然后重启hadoop，这样就能找到驱动类了
 * 缺点：如果是hadoop集群，每个hadoop节点上都要放驱动包，且以后驱动包想升级或者替换都非常麻烦
 *
 * 第二种方法(推荐)
 * export LIBJARS=/home/hadoop/lib/mysql-connector-java-5.1.47.jar
 * export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/home/hadoop/lib/mysql-connector-java-5.1.47.jar
 * hadoop jar  /home/hadoop/lib/mylib/ruozedata-hadoop-1.0.jar  com.tianya.bigdata.hadoop.day20200711.MySQLJarHomeWorkDriver -libjars ${LIBJARS} /tmp/tianyafu/out
 */
public class MySQLJarHomeWorkDriver extends Configured implements Tool {
    /**
     * Execute the command with the given arguments.
     *
     * @param args command specific arguments.
     * @return exit code.
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        String output = args[0];

        // 1 获取Job
        Configuration configuration = super.getConf();


        DBConfiguration.configureDB(configuration,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop:3306/ruozedata",
                "root",
                "root");
        /*String driverClass = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.101.217:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8";
        String userName = "dev";
        String passwd = "lJZx2Ik5eqX3xBDp";
        DBConfiguration.configureDB(configuration,driverClass,dbUrl,userName,passwd);*/

        Job job = Job.getInstance(configuration);

        FileUtils.delete(configuration, output);

        // 2 设置主类
        job.setJarByClass(MySQLJarHomeWorkDriver.class);

        // 3 设置Mapper
        job.setMapperClass(MyMapper.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeptWritable.class);


        // 6 设置输入和输出路径
        String[] fields = new String[]{"deptno","dname","loc"};
        DBInputFormat.setInput(job, DeptWritable.class,"dept",null,null,fields);

        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交Job
        boolean result = job.waitForCompletion(true);
        return result? 0:1;
    }

    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new MySQLJarHomeWorkDriver(), args);
        System.exit(res);
    }

    public static class MyMapper extends Mapper<LongWritable, DeptWritable, NullWritable, DeptWritable> {
        @Override
        protected void map(LongWritable key, DeptWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }
}
