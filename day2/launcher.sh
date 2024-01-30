spark-submit --conf spark.dynamicAllocation.enabled=false \
--master yarn \
--deploy-mode client \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.sql.crossJoin.enabled=true \
--conf spark.sql.adaptive.autoBroadcastJoinThreshold=20495760 \
--conf spark.kryoserializer.buffer.max=512m \
--driver-memory 2g \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 1g \
gs://sparktraining123/deploy_on_cluster.py