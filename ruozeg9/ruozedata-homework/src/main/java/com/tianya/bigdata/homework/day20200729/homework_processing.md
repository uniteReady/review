# 作业过程
```
create database ruozedata;
hdfs dfs -mkdir -p /ruozedata/hive/shop_sales
建表
create external table ruozedata.shop_sales(
shop string comment '商店',
mon string comment '月份',
amount int comment '金额'
) comment '20200729作业——商店销售金额表' row format delimited fields terminated by ',' location '/ruozedata/hive/shop_sales';
hdfs dfs -put /home/hadoop/data/shop_sales.txt /ruozedata/hive/shop_sales

```

# 自连接
```
create table shop_temp as select shop,mon,sum(amount) as mon_amount from shop_sales group by shop,mon;
select a.shop,a.mon,a.mon_amount,sum(b.mon_amount) as agg_sum from shop_temp a join shop_temp b on a.shop = b.shop where b.mon <= a.mon group by a.shop,a.mon,a.mon_amount;

```

# 窗口
```
select t.shop,t.mon,t.mon_amount,sum(t.mon_amount) over(partition by t.shop order by t.mon) from (select shop,mon,sum(amount) mon_amount from shop_sales group by shop,mon) t ;
```