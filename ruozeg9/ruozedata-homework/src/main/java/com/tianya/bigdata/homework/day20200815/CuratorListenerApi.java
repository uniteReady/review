package com.tianya.bigdata.homework.day20200815;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 使用curator实现永久监听
 */
public class CuratorListenerApi {

    String namespace = "ruozedata-workspace";
    String connectString = "hadoop:2181";
    CuratorFramework client = null;
    String nodePath = "/ruozedata-workspace/curator/ruoze/child1";
    String newPath = "/curator/ruoze1/hasChild1";


    @Before
    public void setUp() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,5);
        client = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(10000)
                .connectString(connectString)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        client.start();
    }

    @Test
    public void testCreateNode() throws Exception{
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(newPath+"/ruoze-child2","curator-ruoze".getBytes());
    }

    @Test
    public void testSetData() throws Exception{
        client.setData().forPath(newPath+"/ruoze-child","若泽数据555".getBytes());
    }

    @Test
    public void testDelete() throws Exception{
        client.delete()
                .deletingChildrenIfNeeded()
                //可以指定版本，也可以不指定
//                .withVersion(0)
                .forPath(newPath);
    }
    @Test
    public void testExists() throws Exception{
        Stat stat = client.checkExists().forPath(newPath);
        System.out.println(null != stat);
    }


    @Test
    public void testGetData() throws Exception{
        Stat stat = new Stat();
        byte[] bytes = client.getData().storingStatIn(stat).forPath(newPath);
        System.out.println(new String(bytes));
        System.out.println(stat.getVersion());
    }

    @Test
    public void testListenNode(){
        NodeCache nodeCache = new NodeCache(client,nodePath,false);
        try {
            nodeCache.start();
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    ChildData currentData = nodeCache.getCurrentData();
                    System.out.println(new String(currentData.getData()));
                    System.out.println(currentData.getPath());
                    System.out.println(currentData.getStat());

                }
            });
            Thread.sleep(1000);
            client.create().forPath("/super", "123".getBytes());

            Thread.sleep(1000);
            client.setData().forPath("/super", "456".getBytes());

            Thread.sleep(1000);
            client.delete().forPath("/super");

            Thread.sleep(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    @After
    public void tearDown(){
        if(null != client){
            client.close();
        }
    }

}
