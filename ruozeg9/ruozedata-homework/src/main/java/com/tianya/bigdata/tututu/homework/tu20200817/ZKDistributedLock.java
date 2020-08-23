package com.tianya.bigdata.tututu.homework.tu20200817;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;


public class ZKDistributedLock {

    public static final String LOCK_PATH = "/lock";
    public static final String CONNECT_URL = "hadoop:2181";
    public static final int BASE_SLEEP_TIME_MS = 5000; //定义失败重试间隔时间 单位:毫秒
    public static final int MAX_RETRIES = 3; //定义失败重试次数
    public static final int SESSION_TIME_OUT = 1000000; //定义会话存活时间,根据业务灵活指定 单位:毫秒

    public static void main(String[] args)  {
        CuratorFramework zkClient = getZkClient();
        InterProcessMutex lock = new InterProcessMutex(zkClient, LOCK_PATH);
        //模拟100个线程抢锁
        for (int i = 0; i < 10; i++) {
            new Thread(new TestThread(i, lock)).start();
        }
    }
    static class TestThread implements Runnable {
        private Integer threadFlag;
        private InterProcessMutex lock;

        public TestThread(Integer threadFlag, InterProcessMutex lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                lock.acquire();
                System.out.println("第"+threadFlag+"线程获取到了锁");
                //等到1秒后释放锁
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    lock.release();
                    System.out.println("第"+threadFlag+"线程释放了锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static CuratorFramework getZkClient() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES, SESSION_TIME_OUT);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_URL)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        return zkClient;
    }
}
