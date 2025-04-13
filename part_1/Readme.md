spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --jars mysql-connector-j-8.0.32.jar \
  streaming_pipeline.py