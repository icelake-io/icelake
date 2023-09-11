#!/bin/bash

set -ex

JARS=$(find /opt/spark/deps -type f -name "*.jar" | tr '\n' ':')

/opt/spark/sbin/start-connect-server.sh \
  --master local[3] \
  --driver-class-path $JARS \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=s3a://icebergdata/demo \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=admin \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=password \
  --conf spark.sql.defaultCatalog=demo

tail -f /opt/spark/logs/spark*.out