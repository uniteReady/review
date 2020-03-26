# 错误集

##常见错误集
```$xslt
1.这个错误是spark sql在format jdbc时 传入的options中没有 driver =>com.mysql.jdbc.Driver 即没有指定驱动类  
这个是因为spark官方文档中的示例代码中没有写出 所以容易造成漏写
Exception in thread "main" java.sql.SQLException: No suitable driver
  	at java.sql.DriverManager.getDriver(DriverManager.java:315)
  	at org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$$anonfun$7.apply(JDBCOptions.scala:85)
  	at org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions$$anonfun$7.apply(JDBCOptions.scala:85)
  	at scala.Option.getOrElse(Option.scala:121)
  	at org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.<init>(JDBCOptions.scala:84)
  	at org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.<init>(JDBCOptions.scala:35)
  	at org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider.createRelation(JdbcRelationProvider.scala:34)
  	at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:341)
  	at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:239)
  	at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:227)
  	at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:164)
  	at com.hwinfo.ods.consume.OdsMBaseDept.main(OdsMBaseDept.java:25)
  	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
  	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  	at java.lang.reflect.Method.invoke(Method.java:498)
  	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
  	at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:894)
  	at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:198)
  	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:228)
  	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:137)
  	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
  	
  	
  	
 2.Executor lost
 20/03/17 15:59:13 WARN spark.HeartbeatReceiver: Removing executor 6 with no recent heartbeats: 124051 ms exceeds timeout 120000 ms
 20/03/17 15:59:13 ERROR cluster.YarnScheduler: Lost executor 6 on dsf.host.hz.hwinfo.io: Executor heartbeat timed out after 124051 ms
 这种通过web-ui界面看到是发生了GC导致去Executor上拉取数据时Executor的响应时间超过120s了 导致spark以为Executor lost了
 
```
