package com.tianya.bigdata.hadoop.day20200705;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class HdfsWcApp {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://hadoop:9000");
        FileSystem fileSystem = FileSystem.get(uri,conf,"hadoop");

        Path path = new Path("/ruozedata.txt");

        RuozedataMapper mapper = new WordCountMapper();
        RuozedataContext context = new RuozedataContext();
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(path, false);
        while(remoteIterator.hasNext()){
            LocatedFileStatus fileStatus = remoteIterator.next();
            FSDataInputStream in = fileSystem.open(fileStatus.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while((line = reader.readLine()) != null){
                mapper.map(line,context);
            }
            IOUtils.closeStream(reader);
            IOUtils.closeStream(in);
        }
        HashMap<Object, Object> cacheMap = context.getCacheMap();
        Path outputPath = new Path("/hdfsapi/output/wc.out");
        FSDataOutputStream out = fileSystem.create(outputPath);
        for (Map.Entry<Object, Object> entry : cacheMap.entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
            out.write((entry.getKey()+":"+entry.getValue()+"\n").getBytes());
        }
        fileSystem.close();
    }
}
