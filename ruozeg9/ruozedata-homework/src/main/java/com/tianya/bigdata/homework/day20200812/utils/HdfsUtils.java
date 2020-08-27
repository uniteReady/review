package com.tianya.bigdata.homework.day20200812.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;

public class HdfsUtils implements Serializable {

    public static void deletePath(Configuration conf ,String path) throws Exception{
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(path))){
            fs.delete(new Path(path),true);
        }
    }

    public static void deletePath(String path) throws Exception{
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(path))){
            fs.delete(new Path(path),true);
        }
    }

    public static void createPath(String path) throws Exception{
        FileSystem fs = FileSystem.get(new Configuration());
        if (!fs.exists(new Path(path))){
            fs.mkdirs(new Path(path));
        }
    }


    public static void createNewFile(Configuration conf,String path) throws Exception{
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(new Path(path))){
            fs.createNewFile(new Path(path));
        }
    }


}
