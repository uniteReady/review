drop table if exists  session_aggr_stat;
CREATE TABLE `session_aggr_stat` (
  `task_id` int(11) NOT NULL comment '任务id',
  `session_count` int(11) DEFAULT NULL comment 'session总数',
  `1s_3s` double DEFAULT NULL comment '访问时长在1s_3s的session占比',
  `4s_6s` double DEFAULT NULL comment '访问时长在4s_6s的session占比',
  `7s_9s` double DEFAULT NULL comment '访问时长在7s_9s的session占比',
  `10s_30s` double DEFAULT NULL comment '访问时长在10s_30s的session占比',
  `30s_60s` double DEFAULT NULL comment '访问时长在30s_60s的session占比',
  `1m_3m` double DEFAULT NULL comment '访问时长在1m_3m的session占比',
  `3m_10m` double DEFAULT NULL comment '访问时长在3m_10m的session占比',
  `10m_30m` double DEFAULT NULL comment '访问时长在10m_30m的session占比',
  `30m` double DEFAULT NULL comment '访问时长在30m及以上的session占比',
  `1_3` double DEFAULT NULL comment '访问步长在1_3的session的占比',
  `4_6` double DEFAULT NULL comment '访问步长在4_6的session的占比',
  `7_9` double DEFAULT NULL comment '访问步长在7_9的session的占比',
  `10_30` double DEFAULT NULL comment '访问步长在10_30的session的占比',
  `30_60` double DEFAULT NULL comment '访问步长在30_60的session的占比',
  `60` double DEFAULT NULL comment '访问步长在60及以上的session的占比',
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储第一个功能，session聚合统计的结果';


drop table if exists  session_random_extract;
CREATE TABLE `session_random_extract` (
  `task_id` int(11) NOT NULL comment '任务id',
  `session_id` varchar(255) DEFAULT NULL comment 'session的id',
  `start_time` varchar(50) DEFAULT NULL comment 'session开始时间',
  `search_keywords` varchar(50) DEFAULT NULL comment 'session中搜索过的关键词',
  `click_category_id` varchar(255) DEFAULT NULL comment 'session中点击过的类别的ids',
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储我们的按时间比例随机抽取功能抽取出来的1000个session';


drop table if exists  top10_category;
CREATE TABLE `top10_category` (
  `task_id` int(11) NOT NULL comment '任务id',
  `category_id` int(11) DEFAULT NULL comment '商品品类id',
  `click_count` int(11) DEFAULT NULL comment '该品类点击的次数',
  `order_count` int(11) DEFAULT NULL comment '该品类下单的次数',
  `pay_count` int(11) DEFAULT NULL comment '该品类支付的次数',
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储按点击、下单和支付排序出来的top10品类数据';

drop table if exists  top10_session;
CREATE TABLE `top10_session` (
  `task_id` int(11) NOT NULL comment '任务id',
  `category_id` int(11) DEFAULT NULL comment '商品品类id',
  `session_id` varchar(255) DEFAULT NULL comment 'session的id',
  `click_count` int(11) DEFAULT NULL comment '该session id对这个品类的点击次数',
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储top10每个品类的点击top10的session';

drop table if exists session_detail;
CREATE TABLE `session_detail` (
  `task_id` int(11) NOT NULL COMMENT '任务id',
  `user_id` int(11) DEFAULT NULL COMMENT '用户id',
  `session_id` varchar(255) DEFAULT NULL COMMENT 'session id',
  `page_id` int(11) DEFAULT NULL COMMENT '访问的页面id',
  `action_time` varchar(255) DEFAULT NULL COMMENT '访问页面的时间',
  `search_keyword` varchar(255) DEFAULT NULL COMMENT '搜索的关键词',
  `click_category_id` int(11) DEFAULT NULL COMMENT '点击的某个品类的id',
  `click_product_id` int(11) DEFAULT NULL COMMENT '点击的某个商品的id',
  `order_category_ids` varchar(255) DEFAULT NULL COMMENT '订单中商品品类的ids',
  `order_product_ids` varchar(255) DEFAULT NULL COMMENT '订单中商品的ids',
  `pay_category_ids` varchar(255) DEFAULT NULL COMMENT '一次支付中商品品类的ids',
  `pay_product_ids` varchar(255) DEFAULT NULL COMMENT '一次支付中商品的ids',
  KEY `idx_task_id` (`task_id`),
  KEY `idx_session_id` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用来存储随机抽取出来的session的明细数据、top10品类的session的明细数据';


CREATE TABLE `task` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT comment '任务id 主键自增',
  `task_name` varchar(255) DEFAULT NULL comment '任务名称',
  `create_time` varchar(255) DEFAULT NULL comment '任务创建时间',
  `start_time` varchar(255) DEFAULT NULL comment '任务开始时间',
  `finish_time` varchar(255) DEFAULT NULL comment '任务结束时间',
  `task_type` varchar(255) DEFAULT NULL comment '任务类型',
  `task_status` varchar(255) DEFAULT NULL comment '任务状态',
  `task_param` text comment '任务的参数',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment 'task表，用来存储J2EE平台插入其中的任务的信息';


----------------用户单跳率----------------
CREATE TABLE page_split_convert_rate (
	task_id INT COMMENT '任务id',
	convert_rate VARCHAR (255) COMMENT '用户单跳转换率',
	KEY `idx_task_id` (`task_id`),
	KEY `idx_convert_rate` (`convert_rate`)
) ENGINE = INNODB DEFAULT CHARSET=utf8 COMMENT '页面单跳转换率';

insert into task(task_id,task_name,task_param) values(1,'本地测试任务1','{"startDate":["2015-12-18"],"endDate":["2015-12-18"],"startAge":["23"],"endAge":["50"]}');
insert into task(task_id,task_name,task_param) values(2,'本地测试任务2','{"startDate":["2015-12-18"],"endDate":["2015-12-18"],"targetPageFlow":["1,2,3,4,5,6,7,8,9"]}');

----------------各区域热门商品top3统计----------------
drop  table  if exists  area_top3_product;
create table area_top3_product(
	task_id int comment '任务id',
	area varchar(50) comment '区域',
	area_level varchar(50) comment '区域层级',
	product_id int comment '商品id',
	city_infos varchar(255) comment '城市信息列表',
	click_count int comment '点击次数',
	product_name varchar(50) comment '商品名字',
	product_status varchar(20) comment '商品状态',
	KEY `idx_task_id` (`task_id`)
)engine=innodb charset utf8 comment '各区域热门商品top3统计结果表';

create table city_info(
	city_id int comment '城市id',
	city_name varchar(50) comment '城市名字',
	area varchar(50) comment '区域',
	KEY `idx_city_id` (`city_id`)
)engine=innodb charset utf8 comment '城市信息表';

insert  into `city_info`(`city_id`,`city_name`,`area`) values (0,'北京','华北'),(1,'上海','华东'),(2,'南京','华东'),(3,'广州','华南'),(4,'三亚','华南'),(5,'武汉','华中'),(6,'长沙','华中'),(7,'西安','西北'),(8,'成都','西南'),(9,'哈尔滨','东北');

insert into task(task_id,task_name,task_param) values(3,'本地测试任务3','{"startDate":["2020-03-06"],"endDate":["2020-03-06"]}');
update task set task_param = '{"startDate":["2015-12-20"],"endDate":["2015-12-20"]}' where task_id = 3; -- 线上环境测试时需要改到日期为2015-12-20



----------------广告点击流量统计----------------

CREATE TABLE `ad_user_click_count` (
  `date` varchar(30) DEFAULT NULL comment '日期',
  `user_id` int(11) DEFAULT NULL comment '用户id',
  `ad_id` int(11) DEFAULT NULL comment '广告id',
  `click_count` int(11) DEFAULT NULL comment '点击次数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '用户点击广告次数表';

CREATE TABLE `ad_blacklist` (
  `user_id` int(11) DEFAULT NULL comment '用户id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '黑名单表';

CREATE TABLE `ad_stat` (
  `date` varchar(30) DEFAULT NULL comment '日期',
  `province` varchar(100) DEFAULT NULL comment '省份',
  `city` varchar(100) DEFAULT NULL comment '城市',
  `ad_id` int(11) DEFAULT NULL comment '广告id',
  `click_count` int(11) DEFAULT NULL comment '点击次数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '统计每天每个省份每个城市对于某个广告的点击次数';

CREATE TABLE `ad_province_top3` (
  `date` varchar(30) DEFAULT NULL comment '日期',
  `province` varchar(100) DEFAULT NULL comment '省份',
  `ad_id` int(11) DEFAULT NULL comment '广告id',
  `click_count` int(11) DEFAULT NULL comment '点击次数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '统计每天每个省份广告热门点击的top3';

CREATE TABLE `ad_click_trend` (
  `date` varchar(30) DEFAULT NULL comment '日期',
  `ad_id` int(11) DEFAULT NULL comment '广告id',
  `minute` varchar(30) DEFAULT NULL comment '分钟',
  `click_count` int(11) DEFAULT NULL comment '点击次数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '广告点击趋势表';
