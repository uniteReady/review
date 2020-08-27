#!/bin/sh

#记录脚本开始时间
etlStart=$(date '+%F %R:%S')
etlStartTime=`date +%s`
echo "脚本启动时间:"$etlStart
# 应用环境变量
source ~/.bashrc

# 时间参数
if [ $# -eq 1 ]; then
  time=$1
  echo "脚本参数为:"$time
else
  echo "脚本传参有误，需要传入一个时间参数"
  etlEnd=$(date '+%F %R:%S')
  echo "脚本异常退出，结束时间:"$etlEnd
  exit 1
fi

#记录开始加载时间
start=$(date '+%F %R:%S')

startTime=$(date +%R:%S)
echo "开始ETL任务"$start

/home/hadoop/app/spark/bin/spark-submit \
--name log_etl \
--class com.tianya.bigdata.tututu.homework.tu20200824.SparkETLJob \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--executor-cores 2 \
--num-executors 2 \
--jars /home/hadoop/lib/ip2region-1.7.2.jar,/home/hadoop/app/ruozedata-dw/data/ip2region.db \
/home/hadoop/app/ruozedata-dw/lib/ruozedata-homework-1.0.jar \
/ruozedata/dw/raw/access/$time \
/ruozedata/dw/spark_tmp/access/$time




#记录结束时间
end=`date '+%F %R:%S'`
echo "结束加载"$end
endTime=`date  +%R:%S`

sT=`date +%s -d$startTime`
eT=`date +%s -d$endTime`

#计算脚本耗时
let useTime=`expr $eT - $sT`
echo "ETL任务耗时:"$useTime"秒"

echo "开始移除对应的原有的分区目录"
hdfs dfs -rm -r /ruozedata/dw/spark/access/d=$time

echo "创建对应的分区目录"
hdfs dfs -mkdir -p /ruozedata/dw/spark/access/d=$time

#记录开始加载时间
start1=$(date '+%F %R:%S')

startTime1=$(date +%R:%S)
echo "开始从临时目录中移动数据到对应的分区目录下"$start
hdfs dfs -mv /ruozedata/dw/spark_tmp/access/$time/part* /ruozedata/dw/spark/access/d=$time
#记录结束时间
end1=`date '+%F %R:%S'`
echo "结束移动"$end
endTime1=`date  +%R:%S`

sT1=`date +%s -d$startTime1`
eT1=`date +%s -d$endTime1`

#计算脚本耗时
let useTime1=`expr $eT1 - $sT1`
echo "移动数据耗时:"$useTime1"秒"

echo "删除临时目录及数据"
hdfs dfs -rm -r /ruozedata/dw/spark_tmp/access/$time
echo "关联分区"
hive -e "alter table ruozedata.spark_access add if not exists partition(d='$time');"
#记录脚本开始时间
etlEnd=$(date '+%F %R:%S')
etlEndTime=`date +%s`
etlExecuteTIME=`expr $etlEndTime - $etlStartTime`
echo "脚本结束时间:"$etlEnd",脚本耗时:"$etlExecuteTIME

