create external table ruozedata.dwd_access_province_traffic(
province string,
traffics int,
cnt int
) comment '省份流量表' partitioned by (d string comment '日期分区(yyyyMMdd)')  row format delimited fields terminated by '\t' location '/ruozedata/dw/dwd/access_province_traffic' ;