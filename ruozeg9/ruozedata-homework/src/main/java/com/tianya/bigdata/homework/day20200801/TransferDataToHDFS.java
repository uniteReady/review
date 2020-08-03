package com.tianya.bigdata.homework.day20200801;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 将生成的数据放到组长的HDFS上
 */
public class TransferDataToHDFS {

    //HDFS的uri
//    private static final String HDFS_URI = "hdfs://ruozedata001:9000";
    private static final String HDFS_URI = "hdfs://hadoop01:9000";

    //操作HDFS的用户
    private static final String HDFS_USER = "hadoop";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname","true");
        conf.set("dfs.replication","1");
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URI), conf, HDFS_USER);
        Path src = new Path("out/access.log");
        Path dst = new Path("/G90401/data/access.log");
        fileSystem.copyFromLocalFile(false,true,src,dst);
        fileSystem.close();

    }
}
