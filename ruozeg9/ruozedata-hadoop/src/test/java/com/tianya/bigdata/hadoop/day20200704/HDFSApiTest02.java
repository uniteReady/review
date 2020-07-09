package com.tianya.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用IO流的方式
 */
public class HDFSApiTest02 {

    FileSystem fileSystem;

    @Before
    public void setup() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication","1");
        URI uri = new URI("hdfs://hadoop:9000");
        fileSystem = FileSystem.get(uri,conf,"hadoop");
    }

    @After
    public void tearDown() throws IOException {
        if(null != fileSystem){
            fileSystem.close();
        }
    }


    @Test
    public void copyFromLocalFile() throws Exception {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream("data/ruozedata.txt"));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/pk/io.txt"));
        IOUtils.copyBytes(in,out,4096,true);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/pk/io.txt"));
        FileOutputStream out = new FileOutputStream(new File("out/io-out.txt"));
        IOUtils.copyBytes(in,out,4096,true);
    }


    @Test
    public void copyFromLocalFile2() throws Exception {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream("data/hadoop-2.6.0.tar.gz"));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/pk/hadoop-2.6.0.tar.gz"));
        IOUtils.copyBytes(in,out,4096,true);

    }


}
