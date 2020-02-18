spark2-submit \
--class cn.spark.study.sql.hiveOperation.SparkSqlHiveOperation \
--master yarn \
--deploy-mode client \
--num-executors 1 \
--driver-memory 2G \
--executor-memory 2G \
--executor-cores 4 \
/home/tianyafu/sparktest/scala/spark-zhonghuashishan-1.0.jar \
dsd 8020
