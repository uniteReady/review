#!/bin/sh

/home/hadoop/app/spark/bin/spark-submit \
--name log_etl \
--class com.tianya.bigdata.tututu.homework.tu20200824.SparkETL2 \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 2g \
--executor-cores 2 \
--num-executors 2 \
--jars /home/hadoop/lib/ip2region-1.7.2.jar,hdfs://hadoop01:9000/ruozedata/dw/data/ip2region.db \
/home/hadoop/app/ruozedata-dw/lib/ruozedata-homework-1.0.jar \
/ruozedata/dw/raw/access/20190101 \
/ruozedata/dw/spark/access/d=20190101
