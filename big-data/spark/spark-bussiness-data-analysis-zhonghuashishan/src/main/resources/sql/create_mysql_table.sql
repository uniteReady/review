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
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储第一个功能，session聚合统计的结果';


CREATE TABLE `session_random_extract` (
  `task_id` int(11) NOT NULL comment '任务id',
  `session_id` varchar(255) DEFAULT NULL comment 'session的id',
  `start_time` varchar(50) DEFAULT NULL comment 'session开始时间',
  `end_time` varchar(50) DEFAULT NULL comment 'session结束时间',
  `search_keywords` varchar(255) DEFAULT NULL comment 'session中搜索过的关键词',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储我们的按时间比例随机抽取功能抽取出来的1000个session';


CREATE TABLE `top10_category` (
  `task_id` int(11) NOT NULL comment '任务id',
  `category_id` int(11) DEFAULT NULL comment '商品品类id',
  `click_count` int(11) DEFAULT NULL comment '该品类点击的次数',
  `order_count` int(11) DEFAULT NULL comment '该品类下单的次数',
  `pay_count` int(11) DEFAULT NULL comment '该品类支付的次数',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储按点击、下单和支付排序出来的top10品类数据';


CREATE TABLE `top10_category_session` (
  `task_id` int(11) NOT NULL comment '任务id',
  `category_id` int(11) DEFAULT NULL comment '商品品类id',
  `session_id` varchar(255) DEFAULT NULL comment 'session的id',
  `click_count` int(11) DEFAULT NULL comment '该session id对这个品类的点击次数',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存储top10每个品类的点击top10的session';

CREATE TABLE `session_detail` (
  `task_id` int(11) NOT NULL comment '任务id',
  `user_id` int(11) DEFAULT NULL comment '用户id',
  `session_id` varchar(255) DEFAULT NULL comment 'session id',
  `page_id` int(11) DEFAULT NULL comment '访问的页面id',
  `action_time` varchar(255) DEFAULT NULL comment '访问页面的时间', 
  `search_keyword` varchar(255) DEFAULT NULL comment '搜索的关键词',
  `click_category_id` int(11) DEFAULT NULL comment '点击的某个品类的id',
  `click_product_id` int(11) DEFAULT NULL comment '点击的某个商品的id',
  `order_category_ids` varchar(255) DEFAULT NULL comment '订单中商品品类的ids',
  `order_product_ids` varchar(255) DEFAULT NULL comment '订单中商品的ids',
  `pay_category_ids` varchar(255) DEFAULT NULL comment '一次支付中商品品类的ids',
  `pay_product_ids` varchar(255) DEFAULT NULL comment '一次支付中商品的ids',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '用来存储随机抽取出来的session的明细数据、top10品类的session的明细数据';

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

