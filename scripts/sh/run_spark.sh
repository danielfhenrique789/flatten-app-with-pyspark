#!/bin/bash
/opt/spark/bin/spark-submit \
  --deploy-mode client \
  --jars /opt/spark/jars/postgresql-42.2.27.jar \
  --driver-class-path /opt/spark/jars/postgresql-42.2.27.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.2.27.jar \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.security.authentication=simple \
  --conf "spark.hadoop.security.auth_to_local=RULE:[1:\$1@\${0}](.*@.*)s/@.*//" \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  /app/src/main.py "$@"