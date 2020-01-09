spark2-submit \
  --class cn.spark.study.core.wordCount.WordCountCluster \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 5G \
  --num-executors 4 \
  /home/tianyafu/sparktest/java/spark-zhonghuashishan-1.0.jar
