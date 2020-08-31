package com.tianya.bigdata.tututu.homework.tu20200826;

import com.tianya.bigdata.homework.day20200801.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 小表join大表，mapreduce实现用map side join
 *
 * create external table ruozedata.student_scores(
 * subject_id int,
 * subject string,
 * score string
 * ) comment '学生成绩表，大表' row format delimited fields terminated by '\t' location '/ruozedata/hive/student_scores' ;
 *
 * create external table ruozedata.student_infos(
 * id int,
 * student_name string,
 * subject_id int
 * ) comment '学生信息表,小表' row format delimited fields terminated by '\t' location '/ruozedata/hive/student_infos' ;
 *
 * Hive中实现
 * 设置hive.auto.convert.join参数为true来打开mapjoin
 * 设置hive.mapjoin.smalltable.filesize来告诉hive多大的表算小表
 * select a.subject_id,a.student_name,b.subject,b.score from student_infos a left join student_scores b on a.subject_id = b.subject_id;
 */
public class MapSideJoinMRDriver {

    public static class MyMapper extends Mapper<LongWritable, Text, StudentSubjectScore, NullWritable>{
        private Map<Integer,String> studentInfosMap = new HashMap<>();
        BufferedReader in = null;
        /**
         * 在setup中直接将小表的数据读进来存入一个map中
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheArchives = context.getCacheArchives();
            for (URI uri : cacheArchives) {
                if(uri.getPath().contains("studentInfos")){
                    in = new BufferedReader(new FileReader(uri.getPath()));
                    String line = null;
                    while (null != (line = in.readLine())){
                        String[] splits = line.split("\t");
                        Integer subjectId = Integer.valueOf(splits[2]);
                        String studentName = splits[1];
                        studentInfosMap.put(subjectId,studentName);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            Integer subjectId = Integer.valueOf(splits[0]);
            String subject = splits[1];
            String score = splits[2];
            String studentName = studentInfosMap.get(subjectId);
            context.write(new StudentSubjectScore(subjectId,studentName,subject,score),NullWritable.get());

        }
    }

    public static void main(String[] args) throws Exception {
        String out = "out/studentSubjectScore";
        String bigTable = "out/studentScores.txt";
        //创建一个Job
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        FileUtils.delete(conf,out);
//        job.addCacheArchive(new URI("hdfs://hadoop01:9000/ruozedata/hive/student_infos/studentInfos.txt"));
        job.addCacheArchive(new URI("out/studentInfos.txt"));
        job.setJarByClass(MapSideJoinMRDriver.class);
        job.setMapOutputKeyClass(StudentSubjectScore.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(MyMapper.class);
        FileInputFormat.setInputPaths(job,new Path(bigTable));
        FileOutputFormat.setOutputPath(job,new Path(out));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
