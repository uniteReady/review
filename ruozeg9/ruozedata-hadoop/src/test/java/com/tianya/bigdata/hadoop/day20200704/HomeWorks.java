package com.tianya.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * hdfs dfs -mkdir -p /ruozedata/hdfs-works/20211001
 * hdfs dfs -mkdir -p /ruozedata/hdfs-works/20211002
 * [hadoop@hadoop tmp]$ cd ~/tmp/
 * [hadoop@hadoop tmp]$ echo '20211001_1' > 1.txt
 * [hadoop@hadoop tmp]$ echo '20211001_2' > 2.txt
 * [hadoop@hadoop tmp]$ echo '20211001_3' > 3.txt
 * [hadoop@hadoop tmp]$ hdfs dfs -put ~/tmp/1.txt /ruozedata/hdfs-works/20211001
 * [hadoop@hadoop tmp]$ hdfs dfs -put ~/tmp/2.txt /ruozedata/hdfs-works/20211001
 * [hadoop@hadoop tmp]$ hdfs dfs -put ~/tmp/3.txt /ruozedata/hdfs-works/20211001
 * <p>
 * [hadoop@hadoop tmp]$ echo '20211002_1' > 1.txt
 * [hadoop@hadoop tmp]$ echo '20211002_2' > 2.txt
 * [hadoop@hadoop tmp]$ echo '20211002_3' > 3.txt
 * [hadoop@hadoop tmp]$ hdfs dfs -put ~/tmp/1.txt /ruozedata/hdfs-works/20211002
 * [hadoop@hadoop tmp]$ hdfs dfs -put ~/tmp/2.txt /ruozedata/hdfs-works/20211002
 * [hadoop@hadoop tmp]$ hdfs dfs -put ~/tmp/3.txt /ruozedata/hdfs-works/20211002
 * [hadoop@hadoop tmp]$ hdfs dfs -rm -r /ruozedata/hdfs-works/*
 *
 * hdfs dfs -ls /ruozedata/hdfs-works
 */
public class HomeWorks {

    FileSystem fileSystem;


    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://hadoop:9000");
        fileSystem = FileSystem.get(uri, conf, "hadoop");
    }

    @After
    public void tearDown() throws Exception {
        if (null != fileSystem) {
            fileSystem.close();
        }
    }

    @Test
    public void rename() throws Exception {
        //作业基础路径
        String hdfsBasePath = "/ruozedata/hdfs-works";
        String day = "20211001";
        //根据传入的日期生成相应日期的HDFS路径
        Path srcPath = new Path(hdfsBasePath + File.separator + day);
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(srcPath, true);
        Path copySrcPath;
        Path copyDstPath;
        while (remoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = remoteIterator.next();
            Path path = fileStatus.getPath();
            //获取原文件路径
            String fileSrcPath = path.toString().substring(path.toString().indexOf(hdfsBasePath));
            copySrcPath = new Path(fileSrcPath);
            System.out.println(fileSrcPath);
            //生成新文件路径
            String fileName = path.getName();
            String prefixFileName = fileName.substring(0, fileName.indexOf("."));
            String newPrefixFileName = String.valueOf(Integer.valueOf(prefixFileName)-1);
            String suffixFileName = fileName.substring(fileName.indexOf("."));
//            System.out.println(prefixFileName+"====" + suffixFileName);
            String fileDstPath = path.getParent().toString().substring(path.toString().indexOf(hdfsBasePath))+"-"+newPrefixFileName+suffixFileName;
            System.out.println(fileDstPath);
            copyDstPath = new Path(fileDstPath);
            //rename成目标文件
            fileSystem.rename(copySrcPath,copyDstPath);
        }
        //删除掉原来的空目录
        fileSystem.delete(srcPath,true);
    }



    @Test
    public void download1() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/pk/hadoop-2.6.0.tar.gz"));
        FileOutputStream out = new FileOutputStream(new File("out/hadoop.tgz.part0"));
        byte[] buffer = new byte[2048];
        for (int i = 0; i < 1024*1024 * 128; i+=buffer.length) {
            in.read(buffer);
            out.write(buffer);
        }
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    @Test
    public void download2() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/pk/hadoop-2.6.0.tar.gz"));
        FileOutputStream out = new FileOutputStream(new File("out/hadoop.tgz.part1"));
        in.seek(1024*1024*128);
        IOUtils.copyBytes(in,out,fileSystem.getConf());
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    @Test
    public void merge() throws Exception {
        FileInputStream filePartIn0 = new FileInputStream(new File("out/hadoop.tgz.part0"));
        FileInputStream filePartIn1 = new FileInputStream(new File("out/hadoop.tgz.part1"));
        FileOutputStream out = new FileOutputStream(new File("out/hadoop.tar.gz"));
        IOUtils.copyBytes(filePartIn0,out,4096);
        IOUtils.copyBytes(filePartIn1,out,4096);
        IOUtils.closeStream(filePartIn0);
        IOUtils.closeStream(filePartIn1);
        IOUtils.closeStream(out);
    }

    @Test
    public void merge2() throws Exception {

    }


}
