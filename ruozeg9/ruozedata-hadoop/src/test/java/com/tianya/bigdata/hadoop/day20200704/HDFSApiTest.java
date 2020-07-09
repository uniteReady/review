package com.tianya.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS API测试用例
 *
 * 编程入口点
 *
 */
public class HDFSApiTest {

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
    public void mkdir() throws Exception{
        Path path = new Path("/hdfsapi/pk");
        fileSystem.mkdirs(path);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("data/ruozedata.txt");
        Path dst = new Path("/hdfsapi/pk");
        fileSystem.copyFromLocalFile(src,dst);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/pk/ruozedata.txt");
        Path dst = new Path("out/ruozedata-3.txt");
        fileSystem.copyToLocalFile(true,src,dst);
    }

    /**
     * rename可以是改名，也可以是改名并改路径
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path src = new Path("/hdfsapi/pk/ruozedata.txt");
        Path dst = new Path("/hdfsapi/ruozedata-2.txt");
        fileSystem.rename(src,dst);
    }

    @Test
    public void listFiles() throws Exception {
        Path path = new Path("/hdfsapi/pk");
        RemoteIterator<LocatedFileStatus> filesIterator = fileSystem.listFiles(path, true);
        while(filesIterator.hasNext()){
            LocatedFileStatus fileStatus = filesIterator.next();
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            String owner = fileStatus.getOwner();
            FsPermission permission = fileStatus.getPermission();
            short replication = fileStatus.getReplication();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println(replication + "\n owner:" + owner + "\n permission:" + permission);
        }

    }


    @Test
    public void delete() throws Exception {
        Path path = new Path("/hdfsapi/pk");
        fileSystem.delete(path ,true);
    }
}
