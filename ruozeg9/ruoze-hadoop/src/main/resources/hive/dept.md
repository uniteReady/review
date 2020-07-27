# dept hive 建表
```
hdfs dfs -mkdir -p /ruozedata/hive/dept

create database ruozedata;
use ruozedata;

create external table ruozedata.dept(
deptno int comment '部门编号',
dname string comment '部门名称',
loc string comment '部门所在地'
) comment '部门表' row format delimited fields terminated by '\t' location '/ruozedata/hive/dept';

hdfs dfs -put /home/hadoop/data/dept.txt /ruozedata/hive/dept
```