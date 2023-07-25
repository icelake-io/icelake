#!/usr/bin/env bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJ_DIR="$SCRIPT_DIR/../.."

cd "$SCRIPT_DIR"/docker

docker compose up -d --wait spark

cd "$SCRIPT_DIR"/python
poetry run python init.py -s sc://localhost:15002 -f "$SCRIPT_DIR"/testdata/insert1.csv

"$PROJ_DIR"/target/debug/icelake-integration-tests \
  --s3-bucket icebergdata \
  --s3-endpoint http://localhost:9000 \
  --s3-username admin \
  --s3-password password \
  --s3-region us-east-1 \
  -t "demo/s1/t1" \
  -c "$SCRIPT_DIR"/testdata/insert2.csv



cd "$SCRIPT_DIR"/python
poetry run python check.py -s sc://localhost:15002 -f "$SCRIPT_DIR"/testdata/query1.csv

cd "$SCRIPT_DIR"/docker
docker compose down -v --remove-orphans
