# emp hive 建表
```
hdfs dfs -mkdir -p /ruozedata/hive/emp

use ruozedata;
create external table ruozedata.emp(
empno int comment '员工编号',
ename string comment '员工名称',
job string comment '职位',
mgr int comment '直属领导编号',
hiredate string comment '入职时间',
sal string comment '工资',
comm string comment '奖金',
deptno int comment '部门号'
) comment '员工表' row format delimited fields terminated by '\t' location '/ruozedata/hive/emp';

hdfs dfs -put /home/hadoop/data/emp.txt /ruozedata/hive/emp
```