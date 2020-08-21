package com.tianya.bigdata.tututu.homework.tu20200817;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZKClientUtils {

    private static final int BASE_SLEEP_TIME_MS = 5000; //定义失败重试间隔时间 单位:毫秒
    private static final int MAX_RETRIES = 3; //定义失败重试次数
    private static final int SESSION_TIME_OUT = 1000000; //定义会话存活时间,根据业务灵活指定 单位:毫秒
    private static final String ZK_URI = "hadoop:2181";//你自己的zkurl和端口号
    private static final String NAMESPACE = "ruozedata-workspace";
    //工作空间,可以不指定,建议指定,功能类似于项目包,之后创建的所有的节点都会在该工作空间下,方便管理

    public static CuratorFramework build(){
        //创建比较简单,链式编程,很爽,基本上指定点参数就OK了
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS,MAX_RETRIES);//重试策略
        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString(ZK_URI)
                .retryPolicy(retryPolicy)
                .namespace(NAMESPACE)
                .sessionTimeoutMs(SESSION_TIME_OUT)
                .build();
        return client;
    }

}
