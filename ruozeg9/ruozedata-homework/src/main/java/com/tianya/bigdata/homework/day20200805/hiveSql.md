```$xslt

请使用scala完成日志统计分析大作业中的两个SQL的功能
https://www.bilibili.com/video/BV1pt411176m
==> 总结出视频的面试点
pk,2021-09-01,500,10.10.10.9   
xingxing,2021-09-02,3500,10.10.10.10
pk,2021-02-03,46,10.10.10.9
xingxing,2021-09-04,578,10.10.10.10
pk,2021-09-05,345,10.10.10.9
pk,2021-04-06,235,10.10.10.9
xingxing,2021-09-07,78,10.10.10.10
pk,2021-09-08,55,10.10.10.9
zhangsan,2021-04-08,783,10.10.10.11
zhangsan,2021-04-09,123,10.10.10.11
lisi,2021-05-10,150,10.10.10.12
zhangsan,2021-04-11,456,10.10.10.11
lisi,2021-06-12,234,10.10.10.12
zhangsan,2021-04-13,99,10.10.10.11
用户购买明细及上次的购买时间
用户购买明细及下次的购买时间
用户购买明细及本月第一次购买的时间
用户购买明细及本月最后一次购买的时间
用户购买明细及每月总额
用户购买明细及金额按日期累加
用户购买明细及最近三次的总额
查询前30%时间的订单信息
```

```$xslt
hdfs dfs -mkdir -p /ruozedata/hive/order_info

create external table ruozedata.order_info(
name string comment '用户',
order_time string comment '时间',
money string comment '消费金额',
ip string comment 'ip地址'
) comment '20200805作业_窗口函数练习题' row format delimited fields terminated by ',' location '/ruozedata/hive/order_info';

hdfs dfs -put /home/hadoop/data/order_info.txt /ruozedata/hive/order_info
```

```

select name,order_time,money,ip,
lag(order_time,1,null) over(partition by name order by order_time) as previous_time, -- 用户购买明细及上次的购买时间
lead(order_time,1,null) over(partition by name order by order_time) as next_time, -- 用户购买明细及下次的购买时间
first_value(order_time,true) over(partition by name,from_unixtime(to_unix_timestamp(order_time,'yyyy-MM-dd'),'yyyy-MM') order by order_time)  as month_first_time, -- 用户购买明细及本月第一次购买的时间
first_value(order_time,true) over(partition by name,from_unixtime(to_unix_timestamp(order_time,'yyyy-MM-dd'),'yyyy-MM') order by order_time desc)  as month_last_time, -- 用户购买明细及本月最后一次购买的时间
sum(money) over(partition by name,from_unixtime(to_unix_timestamp(order_time,'yyyy-MM-dd'),'yyyy-MM')) as month_total_money, -- 用户购买明细及每月总额
sum(money) over(partition by name order by order_time ) as daliy_sum_money, -- 用户购买明细及金额按日期累加
sum(money) over(partition by name order by order_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) sum1, -- 用户购买明细及最近三次的总额(当前行与前2行)
sum(money) over(partition by name order by order_time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) sum2, -- 用户购买明细及最近三次的总额(当前行与前后各1行)
sum(money) over(partition by name order by order_time ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) sum3 -- 用户购买明细及最近三次的总额(当前行与后2行)
 from ruozedata.order_info order by name,order_time ;


-- 查询前30%时间的订单信息
 select name,order_time,money,ip,order_time_percent 
 from 
(select name,order_time,money,ip,
round(CUME_DIST() over(order by order_time ),2) as order_time_percent
 from ruozedata.order_info ) t where t.order_time_percent <=0.3 ;
```