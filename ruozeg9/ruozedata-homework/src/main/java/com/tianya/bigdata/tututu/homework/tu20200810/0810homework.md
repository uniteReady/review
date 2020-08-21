# 用hive实现cities的图片功能
```$xslt
hdfs dfs -mkdir -p /ruozedata/hive/address
create external table ruozedata.address(
id int comment 'id',
name string comment '城市',
parentId int comment '父id'
) comment '20200810作业' row format delimited fields terminated by ',' location '/ruozedata/hive/address';
hdfs dfs -put /home/hadoop/data/address.txt /ruozedata/hive/address


-- 自连接方式
select a.name first_level,b.name second_level,c.name third_level from 
address a join address b on a.id = b.parentid join address c on b.id = c.parentid where a.parentid = 0;

```