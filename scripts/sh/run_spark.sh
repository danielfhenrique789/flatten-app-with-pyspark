#!/bin/bash

#rm -rf /app/data/bronze/raw.parquet
#mkdir -p /app/data/bronze
#chmod -R 777 /app/data/bronze

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --jars /opt/spark/jars/postgresql-42.2.27.jar \
  --driver-class-path /opt/spark/jars/postgresql-42.2.27.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.2.27.jar \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.security.authentication=simple \
  --conf "spark.hadoop.security.auth_to_local=RULE:[1:\$1@\${0}](.*@.*)s/@.*//" \
  --packages org.apache.hadoop:hadoop-aws:3.3.2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=true \
  --conf spark.ui.port=4040 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  /app/src/main.py "$@"