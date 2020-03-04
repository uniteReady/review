spark-submit \
--class cn.spark.study.project.UserVisitActionDataLoadTest \
--num-executors 1 \
--driver-memory 1g \
--executor-memory 600M \
--executor-cores 1 \
--master spark://master:7077 \
--deploy-mode client \
--jars hdfs://master:9000/tmp/tianyafu/mysql-connector-java-5.1.45.jar \
/root/tianyafu/spark/spark-bussiness-data-analysis-1.0.jar
