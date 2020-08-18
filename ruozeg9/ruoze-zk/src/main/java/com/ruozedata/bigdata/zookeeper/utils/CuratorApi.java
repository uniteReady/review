package com.ruozedata.bigdata.zookeeper.utils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorApi {

    CuratorFramework client = null;
    String zkQuorum = "hadoop01:2181";
    String nodePath = "/curator/ruoze";
    String nodePath1 = "/curator/ruoze/child1";
    String nodePath2 = "/curator/ruoze/child2";
    String nodePath3 = "/curator/ruoze/child2/child3";

    @Before
    public void setUp(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,5);
        client = CuratorFrameworkFactory.builder()
                .connectString(zkQuorum)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .namespace("ruozedata-workspace")
                .build();
        client.start();
    }

    @Test
    public void testCreateNode() throws Exception{
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(nodePath2,"curator-ruoze".getBytes());
    }

    @Test
    public void testSetData() throws Exception{
        client.setData().forPath(nodePath,"若泽数据".getBytes());
    }

    @Test
    public void testGetData() throws Exception{
        Stat stat = new Stat();
        byte[] bytes = client.getData().storingStatIn(stat).forPath(nodePath);
        System.out.println(new String(bytes));
        System.out.println(stat.getVersion());
    }

    @Test
    public void testExists() throws Exception{
        Stat stat = client.checkExists().forPath(nodePath);
        System.out.println(null != stat);
    }

    @Test
    public void testGetChildren() throws Exception{
        List<String> childrens = client.getChildren().forPath(nodePath);
        for (String children : childrens) {
            System.out.println(children);
        }
    }

    @Test
    public void testDelete() throws Exception{
        client.delete()
                .deletingChildrenIfNeeded()
                //可以指定版本，也可以不指定
                .withVersion(0)
                .forPath(nodePath2);
    }


    @After
    public void tearDown(){
        if(null != client){
            client.close();
        }
    }
}















