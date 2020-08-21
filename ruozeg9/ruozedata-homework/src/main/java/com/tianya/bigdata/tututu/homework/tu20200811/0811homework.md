# 梳理Hadoop中常用的端口号以及对应的配置参数名称
# 简述Hadoop单机部署以及HA部署的核心步骤
# HDFS读流程和写流程：请使用自己的语言整理，别长篇大论的，最好的方式是一边画图一边默写出读写流程

## 梳理Hadoop中常用的端口号以及对应的配置参数名称
```$xslt
常用的
50070 dfs.namenode.http-address         namenode web ui 的ip 和 port
50090 dfs.namenode.secondary.http-address       secondaryNameNode的http ip 和 port
50075 dfs.datanode.http.address         datanode http server的ip 和 port
50010 dfs.datanode.address              dataNode之间数据传输的ip 和 port
8480  dfs.journalnode.http-address      journalnode http server的ip 和 port
8020/9000 fs.defaultFS                  filesystem的ip 和 port
8032 yarn.resourcemanager.address       resourcemanager中的application manager的 port
8030 yarn.resourcemanager.scheduler.address    resourcemanager中的scheduler 的 port 
8088 yarn.resourcemanager.webapp.address       resourcemanager中的web ui 的 port
19888 mapreduce.jobhistory.webapp.address      MapReduce JobHistory Server Web UI host:port 
```

```$xslt
全部的
org.apache.hadoop:hadoop-hdfs:2.6.0-cdh5.16.2 hdfs-default.xml
dfs.namenode.secondary.http-address 0.0.0.0:50090 secondaryNameNode的http ip 和 port
dfs.namenode.secondary.https-address 0.0.0.0:50091 secondaryNameNode的https ip 和 port
dfs.datanode.address 0.0.0.0:50010 dataNode之间数据传输的ip 和 port
dfs.datanode.http.address 0.0.0.0:50075 datanode http 服务的ip 和 port
dfs.datanode.https.address 0.0.0.0:50475  datanode https 服务的ip 和 port
dfs.datanode.ipc.address 0.0.0.0:50020 datanode ipc的ip 和 port
dfs.namenode.http-address 0.0.0.0:50070 namenode web ui 的ip 和 port
dfs.namenode.https-address 0.0.0.0:50470 namenode https 的 ip 和 port
dfs.namenode.backup.address 0.0.0.0:50100 backup的namenode 的ip 和 port
dfs.namenode.backup.http-address 0.0.0.0:50105 backup的namenode 的http的ip 和 port
nfs.server.port 2049 Hadoop NFS使用的 port
nfs.mountd.port 4242 Hadoop 安装守护程序的 port
dfs.journalnode.rpc-address 0.0.0.0:8485 JournalNode RPC server address and port
dfs.journalnode.http-address 0.0.0.0:8480 The address and port the JournalNode HTTP server listens on
dfs.journalnode.https-address 0.0.0.0:8481 The address and port the JournalNode HTTPS server listens on

org.apache.hadoop:hadoop-common:2.6.0-cdh5.16.2 core-default.xml
fs.defaultFS 8020/9000 filesystem 的 ip 和 port
fs.ftp.host.port 21 FTP filesystem connects to fs.ftp.host on this port

org.apache.hadoop:hadoop-yarn-common:2.6.0-cdh5.16.2 yarn-default.xml
yarn.resourcemanager.address ${yarn.resourcemanager.hostname}:8032 resourcemanager中的application manager的 port
yarn.resourcemanager.scheduler.address ${yarn.resourcemanager.hostname}:8030 resourcemanager中的scheduler 的 port
yarn.resourcemanager.webapp.address ${yarn.resourcemanager.hostname}:8088 resourcemanager中的web ui 的 port
yarn.resourcemanager.webapp.https.address ${yarn.resourcemanager.hostname}:8090 resourcemanager中的web ui 的 https port
yarn.resourcemanager.resource-tracker.address ${yarn.resourcemanager.hostname}:8031 resourcemanager中的resource-tracker的 port
yarn.resourcemanager.admin.address ${yarn.resourcemanager.hostname}:8033 resourcemanager 的 admin 的 port
yarn.nodemanager.address ${yarn.nodemanager.hostname}:0 nodemanager中的 container manager的port
yarn.nodemanager.localizer.address ${yarn.nodemanager.hostname}:8040 本地化程序的IPC port
yarn.nodemanager.webapp.address ${yarn.nodemanager.hostname}:8042 nodemanager的web ui 的port
hadoop.registry.zk.quorum localhost:2181 连接zk的端口

org.apache.hadoop:hadoop-mapreduce-client-core:2.6.0-cdh5.16.2 mapred-default.xml
mapreduce.jobtracker.http.address 0.0.0.0:50030 job tracker http server 监听的ip 和 port
mapreduce.tasktracker.http.address 0.0.0.0:50060 task tracker http server address and port
mapreduce.shuffle.port 13562 shuffle 的port
mapreduce.jobhistory.address 0.0.0.0:10020  MapReduce JobHistory Server IPC host:port
mapreduce.jobhistory.webapp.address 0.0.0.0:19888 MapReduce JobHistory Server Web UI host:port
mapreduce.jobhistory.admin.address 0.0.0.0:10033 The address of the History server admin interface

```

## 简述Hadoop单机部署以及HA部署的核心步骤
## 单机部署
```$xslt
将Hadoop解压之后 配置服务器的信任关系
配置6个hadoop的配置文件 hadoop-env.sh、core-site.xml、hdfs-site.xml、slaves、yarn-site.xml、mapred-site.xml
hadoop-env.sh中显式指定JAVA_HOME
core-site.xml中指定分布式文件系统的ip 和 port
hdfs-site.xml中指定副本数为1，配置 secondary namenode的http和https的端口
配置slaves文件，指定slave节点
在mapred-site.xml中，指定mapreduce跑在yarn上
在yarn-site.xml中，指定mapreduce的shuffle 服务为mapreduce_shuffle，并修改resourcemanager的web 应用的ip和端口，最好把端口改掉，防止被挖矿
配置完配置文件后，格式化namenode
最后配置环境变量
```

## HA部署
```$xslt
准备3台及以上服务器，数量为奇数台
机器之间配置信任关系
配置zookeeper并启动
解压hadoop，并配置6个配置文件
配置完后先启动journalnode，然后进行namenode的格式化
格式化之后，将namenode的元数据同步拷贝到备用namenode的相关目录下
然后初始化ZFCK
将hadoop配置到环境变量中，然后分别启动hdfs、yarn、jobhistory

```

# HDFS读流程和写流程：请使用自己的语言整理，别长篇大论的，最好的方式是一边画图一边默写出读写流程
## HDFS读流程
```$xslt

1.client调用DistributedFileSystem的open方法，来打开需要读取的文件
2.DistributedFileSystem会通过RPC请求，向namenode申请文件的block信息，namenode会根据client所在的集群的网络拓扑情况，将block的信息返回给client
并返回一个FSDataInputStream对象
3.Client调用FSDataInputStream的read方法，去离Client最近的一个DataNode读取第一个block块的数据，读完之后校验，校验通过之后关闭与该DataNode的连接
并转而读取下一个block的数据，这样依次读取，知道读取完所有block的数据
4.Client调用FSDataInputStream的close方法关闭连接
```

## HDFS写流程
```
1.Client调用DistributedFileSystem的create方法来创建文件
2.DistributedFileSystem通过RPC请求向namenode发送请求，在filesystem的namespace中创建文件，namenode会检查该client是否有权限，文件是否存在
然后返回给Client一个FSDataOutputStream对象
3.Client调用DistributedFileSystem的write方法来开始写数据
4.Client会将数据拆分成一个一个的数据包，将数据包加入到一个数据队列中，并同时创建一个ACK确认队列。
5.Client会为数据队列中的当前数据包申请一组DataNode节点，并连接这些DataNode形成一条pipeline，然后FSDataOutputStream会将当前数据包写入到第一个DataNode中
这个DN写入数据，并将数据传输给第二个DN，第二个DN写入数据并将数据传输给第三个DN
6.第三个DN写入数据后，向第二个DN返回ACK，第二个收到ACK之后向第一个DN返回ACK，第一个DN收到ACK后向FSDataOutputStream返回ACK，FSDataOutputStream收到
ACK之后，从ACK确认队列中移除，表示这个数据包已经写入
7.Client等待所有数据包都写入之后，调用FSDataOutputStream的close方法来结束写入，并告知namenode写入完成


```
