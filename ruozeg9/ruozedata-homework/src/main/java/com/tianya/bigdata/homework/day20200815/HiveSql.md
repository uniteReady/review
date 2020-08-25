```$xslt
8月30号  分组实现
PV/UV 
	区域分布PV/UV
		province    count(distinct xxx)

	运营商PV/UV占比
Top客户端IP	20
	流量 response_size
	请求次数 1
地区和运营商	
	总流量、总流量占比
热门URL  top20	
	访问次数、访问占比
with t as 
(select 
url,
-- url开窗，每个窗口的url 个数
count(url) over(partition by url ) cnt,
-- 所有数据为一个窗口，求出数据的总个数
count(url) over() total 
from ruozedata.ods_access) 
select t.url,t.cnt,t.total,round(t.cnt*100/t.total,2) as ratio from t 
group by t.url,t.cnt,t.total order by t.cnt desc limit 20;

求每个域名下访问次数最高的path  20
```