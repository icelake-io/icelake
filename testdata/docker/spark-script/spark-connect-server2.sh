#!/bin/bash

set -ex

JARS=$(find /opt/spark/deps -type f -name "*.jar" | tr '\n' ':')

/opt/spark/sbin/start-connect-server.sh  \
  --master local[3] \
  --driver-class-path $JARS \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.demo.uri=http://rest:8181 \
  --conf spark.sql.catalog.demo.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.demo.s3.path.style.access=true \
  --conf spark.sql.catalog.demo.s3.access.key=admin \
  --conf spark.sql.catalog.demo.s3.secret.key=password \
  --conf spark.sql.defaultCatalog=demo

tail -f /opt/spark/logs/spark*.out