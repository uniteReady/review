package com.tianya.bigdata.tututu.homework.tu20200817;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZKDistributedLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static CountDownLatch countDownLatch = new CountDownLatch(1);
    public static CuratorFramework client = null;
    public static String namespace = "ruozedata-workspace";
    public static String connectString = "hadoop:2181";
    public static String lockParentPath = "zk-lock";
    public static String lockName = "distribute-lock";


    static {
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
     * 获取锁，得不到锁就等待
     */
    public static void getLock(){
        while (true){
            try {
                client.create().withMode(CreateMode.EPHEMERAL).forPath(lockParentPath+"/"+lockName);
                LOGGER.info("获取锁成功");
                return;
            } catch (Exception e) {
                if(countDownLatch.getCount()<=0){
                    countDownLatch = new CountDownLatch(1);
                }
                try {
                    LOGGER.info("获取锁失败，等待其他人释放锁");
                    countDownLatch.await();
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
    }

    /**
     * 释放锁
     * @param path
     */
    public static void releaseLock(String path){
        try {
            client.delete().forPath(path);
            LOGGER.info("释放锁成功");
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("释放锁失败");
        }

    }

    /**
     * 添加观察者来监听锁节点
     * @param path
     */
    public static void addWatch(String path){
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, true);
        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            LOGGER.info("创建观察者成功");
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
                        //观察的路径没了，也就是锁释放掉了
                        String removedPath = event.getData().getPath();
                        LOGGER.info("锁已释放,znode节点路径为:"+removedPath);
                        if(removedPath.contains(lockName)){
                            //移除的节点路径中包含了锁路径名
                            countDownLatch.countDown();
                            LOGGER.info("计数器释放,计数器当前计数为:"+countDownLatch.getCount());
                        }

                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final ExecutorService threadpool = Executors.newCachedThreadPool();
        System.out.println("开始购买...");
        for (int i = 0; i <2 ; i++) {
            threadpool.execute(new Runnable() {
                public void run() {
                    System.out.println("我是线程:"+Thread.currentThread().getName()+"我开始抢购了...");
                    getLock();
                    System.out.println(Thread.currentThread().getName()+":我正在疯狂的剁手购买中...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+":我买完了,有请下一位...");
                    try {
                        addWatch("/zk-lock");
                        System.out.println("添加完毕...");
                        releaseLock("/zk-lock/distribute-lock");
                        System.out.println("释放完毕...");
                        Thread.sleep(1000);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }











}
