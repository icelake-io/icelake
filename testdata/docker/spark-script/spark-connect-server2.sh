#!/bin/bash

set -ex

JARS=$(find /opt/spark/deps -type f -name "*.jar" | tr '\n' ':')

/opt/spark/sbin/start-connect-server.sh  \
  --master local[3] \
  --driver-class-path $JARS \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.demo.uri=http://rest:8181 \
  --conf spark.sql.defaultCatalog=demo

tail -f /opt/spark/logs/spark*.out