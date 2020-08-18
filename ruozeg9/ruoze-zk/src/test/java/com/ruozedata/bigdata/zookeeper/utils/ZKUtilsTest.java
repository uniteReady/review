package com.ruozedata.bigdata.zookeeper.utils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZKUtilsTest {

    ZooKeeper zk = null;
    String connnect = "hadoop01:2181";
    int timeout = 5000;
    String path = "/ruozedata";
    String path2 = "/ruozedata/child1";
    String path3 = "/ruozedata/child2";
    String path4 = "/ruozedata/child2/child3";

    @Before
    public void setUp(){
        try {
            zk = new ZooKeeper(connnect, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {

                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConnect(){
        Assert.assertNotNull(zk);
    }

    @Test
    public void testCreateNode(){
        try {
            ZKUtils.createNode(path4,"若泽数据",zk);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData(){
        try {
            System.out.println(ZKUtils.getData(path, zk));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetData(){
        try {
            System.out.println(ZKUtils.updateData(path, "pk123456", zk));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExists(){
        try {
            System.out.println(ZKUtils.exists(path4, zk));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testgetChildren(){
        try {
            List<String> childrens = ZKUtils.getChildren(path, zk);
            for (String children : childrens) {
                System.out.println(children);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDelete(){
        try {
            ZKUtils.delete(path4, zk);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
