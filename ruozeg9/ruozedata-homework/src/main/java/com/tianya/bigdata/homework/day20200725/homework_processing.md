# 作业处理过程
```
drop table if exists ruozedata.order;
create external table ruozedata.order(
name string comment '姓名',
order_time string comment '下单时间'
) comment '订单表' row format delimited fields terminated by '\t' location '/ruozedata/hive/order';
hdfs dfs -put /home/hadoop/data/order.txt /ruozedata/hive/order


drop table if exists ruozedata.pick_up;
create external table ruozedata.pick_up(
name string comment '姓名',
pick_time string comment '取件时间'
) comment '取件表' row format delimited fields terminated by '\t' location '/ruozedata/hive/pick_up';
hdfs dfs -put /home/hadoop/data/pick_up.txt /ruozedata/hive/pick_up

drop table if exists ruozedata.delivery_goods;
create external table ruozedata.delivery_goods(
name string comment '姓名',
delivery_goods_time string comment '送货时间'
) comment '送货表' row format delimited fields terminated by '\t' location '/ruozedata/hive/delivery_goods';
hdfs dfs -put /home/hadoop/data/delivery_goods.txt /ruozedata/hive/delivery_goods

drop table if exists ruozedata.delivery_reach;
create external table ruozedata.delivery_reach(
name string comment '姓名',
delivery_reach_time string comment '送达时间'
) comment '送达表' row format delimited fields terminated by '\t' location '/ruozedata/hive/delivery_reach';
hdfs dfs -put /home/hadoop/data/delivery_reach.txt /ruozedata/hive/delivery_reach
```

```
create table tab_temp1 as 
select
 a.name,a.order_time,b.pick_time,c.delivery_goods_time,d.delivery_reach_time
from 
    order a 
left join 
    pick_up b 
on a.name = b.name
left join 
    delivery_goods c 
on a.name = c.name 
left join 
    delivery_reach d 
on a.name = d.name;

 with t as  (select 
					name,
					unix_timestamp(pick_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(order_time,'yyyy-MM-dd HH:mm:ss')  pick_up_interval,
					unix_timestamp(delivery_goods_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(order_time,'yyyy-MM-dd HH:mm:ss')  delivery_goods_interval,
					unix_timestamp(delivery_reach_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(order_time,'yyyy-MM-dd HH:mm:ss')  delivery_reach_interval 
				from tab_temp1) 
 select 
	t.name 
from 
	t  
where 
	t.pick_up_interval>=2*3600 
	or 
	t.delivery_goods_interval >=4*3600 
	or 
	t.delivery_reach_interval >= 48*3600;
```