package com.ruozedata.bigdata.zookeeper.utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;


public class ZKUtils {

    public static ZooKeeper getZK(String connect,int timeout) throws Exception {
        return new ZooKeeper(connect,timeout,null);
    }

    public static String createNode(String path,String value,ZooKeeper zk) throws Exception {
        return zk.create(path,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static String getData(String path ,ZooKeeper zk) throws Exception{
        return new String(zk.getData(path,false,null));
    }

    public static boolean updateData(String path,String value,ZooKeeper zk) throws Exception{
        zk.setData(path,value.getBytes(),-1);
        String data = getData(path, zk);
        return value.equals(data);
    }

    public static boolean exists(String path,ZooKeeper zk) throws Exception{
        return zk.exists(path,false)!=null;
    }

    public static List<String> getChildren(String path,ZooKeeper zk) throws Exception{
        return zk.getChildren(path,false);
    }

    public static void delete(String path,ZooKeeper zk) throws Exception{
        zk.delete(path,-1);
    }


}
