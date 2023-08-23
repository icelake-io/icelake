#!/bin/bash

set -ex

/usr/bin/mc config host add minio http://"$MINIO_IP":9000 admin password;
/usr/bin/mc rm -r --force minio/icebergdata || true;
/usr/bin/mc mb minio/icebergdata/demo;
/usr/bin/mc policy set public minio/icebergdata/demo;

echo "MC Done"