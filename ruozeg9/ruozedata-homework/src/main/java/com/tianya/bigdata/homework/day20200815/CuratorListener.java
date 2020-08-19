package com.tianya.bigdata.homework.day20200815;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorListener {

    public static CuratorFramework client = null;


    static {
        String namespace = "ruozedata-workspace";
        String connectString = "hadoop:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,5);
        client = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(10000)
                .connectString(connectString)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        client.start();
    }


    /**
     * 监听一个目录
     * 如果该目录不存在，会自动创建
     * set 这个目录的值是不会监听到的
     * 能监听 create 子节点、set 子节点的data 、delete 子节点、delete自身
     * @param nodePath
     */
    public static void listenPathForever(String nodePath){
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,nodePath,true);
        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    ChildData currentData = event.getData();
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            printZnodeInfo(currentData);
                            break;
                        case CHILD_UPDATED:
                            printZnodeInfo(currentData);
                            break;
                        case CHILD_REMOVED:
                            printZnodeInfo(currentData);
                            break;
                        default:
                            break;
                    }


                }
            });

            while (true){

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 监听某个节点 的 创建、set值、删除节点的操作
     * @param nodePath
     */
    public static void listenNodeForever(String nodePath){
        /*RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,5);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectionTimeoutMs(10000)
                .connectString(connectString)
                .retryPolicy(retryPolicy)
                .namespace(namespace)
                .build();
        client.start();*/
        NodeCache nodeCache = new NodeCache(client,nodePath,false);
        try {
            nodeCache.start();
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    ChildData currentData = nodeCache.getCurrentData();
                    printZnodeInfo(currentData);
                }
            });
            while (true){

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printZnodeInfo(ChildData currentData){
        System.out.println(new String(currentData.getData()));
        System.out.println(currentData.getPath());
        System.out.println(currentData.getStat());
    }

    public static void main(String[] args) {

        /*String nodePath = "/curator/ruoze/child1";
        listenNodeForever(nodePath);*/

        String nodePathHasChild = "/curator/ruoze1/hasChild1";
        listenPathForever(nodePathHasChild);
    }
}
