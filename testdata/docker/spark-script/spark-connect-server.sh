#!/bin/bash

set -ex

#HTTP_PROXY=socks5://192.168.110.27:7890
#HTTPS_PROXY=socks5://192.168.110.27:7890
#  --conf spark.driver.extraJavaOptions="-Dhttps.proxyHost=192.168.110.27 -Dhttps.proxyPort=7890" \

ICEBERG_VERSION=1.3.1
SPARK_VERSION=3.4.1

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.3.2"
PACKAGES="$PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"

/opt/spark/sbin/start-connect-server.sh --packages $PACKAGES \
  --master local[3] \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=s3a://icebergdata/demo \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://"$MINIO_IP":9000 \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=admin \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=password \
  --conf spark.sql.defaultCatalog=demo

tail -f /opt/spark/logs/spark*.out