#!/bin/bash

set -ex

# HTTP_PROXY=socks5://192.168.110.27:7890
# HTTPS_PROXY=socks5://192.168.110.27:7890
#  --conf spark.driver.extraJavaOptions="-Dhttps.proxyHost=192.168.110.27 -Dhttps.proxyPort=7890" \

ICEBERG_VERSION=1.3.1
SPARK_VERSION=3.4.1

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:$ICEBERG_VERSION"
PACKAGES="$PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"

# add AWS dependency
AWS_SDK_VERSION=2.20.18
AWS_MAVEN_GROUP=software.amazon.awssdk
AWS_PACKAGES=(
    "bundle"
)
for pkg in "${AWS_PACKAGES[@]}"; do
    PACKAGES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

echo "$PACKAGES"

/opt/spark/sbin/start-connect-server.sh --packages $PACKAGES \
  --master local[3] \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.demo.uri=http://rest:8181 \
  --conf spark.sql.defaultCatalog=demo

tail -f /opt/spark/logs/spark*.out