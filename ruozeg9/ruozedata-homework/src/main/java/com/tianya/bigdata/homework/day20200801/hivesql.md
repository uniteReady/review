# hivesql

```
create external table ruozedata.access_log_partition_temp(
access_time string comment '访问时间',
ip string comment 'ip',
country string comment '国家',
region string comment '区域',
province string comment '省份',
city string comment '城市',
isp string comment 'ISP',
agent_ip string comment '代理IP',
response_time int comment '响应时间',
referer string comment 'referer',
method  string comment '请求类型',
url string comment '请求url',
http_code int comment 'http状态码',
request_size int comment '请求大小',
response_size int comment '响应大小',
cache_status string comment '命中缓存的状态',
ua_head string comment 'UA头',
type string comment '文件类型',
partition_date string comment '日期分区(yyyyMMdd)'
) comment '访问日志数据临时表' row format delimited fields terminated by '|' location '/G90401/etl_temp';



create external table ruozedata.access_log_partition(
access_time string comment '访问时间',
ip string comment 'ip',
country string comment '国家',
region string comment '区域',
province string comment '省份',
city string comment '城市',
isp string comment 'ISP',
agent_ip string comment '代理IP',
response_time int comment '响应时间',
referer string comment 'referer',
method  string comment '请求类型',
url string comment '请求url',
http_code int comment 'http状态码',
request_size int comment '请求大小',
response_size int comment '响应大小',
cache_status string comment '命中缓存的状态',
ua_head string comment 'UA头',
type string comment '文件类型'
) comment '访问日志数据表' partitioned by (partition_date string comment '日期分区(yyyyMMdd)')  row format delimited fields terminated by '|' location '/G90401/etl' ;


insert overwrite table ruozedata.access_log_partition partition(partition_date) select access_time,ip,country,region,province,city,isp,agent_ip,response_time,referer,method,url,http_code,request_size,response_size,cache_status,ua_head,type,partition_date from ruozedata.access_log_partition_temp;




select province,sum(request_size) traffics,count(1) cnt from access_log_partition group by province;


```