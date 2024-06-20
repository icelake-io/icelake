#!/bin/bash

set -ex

docker build --network host -t icelake-spark .
docker tag icelake-spark ghcr.io/icelake-io/icelake-spark:0.1
docker tag icelake-spark ghcr.io/icelake-io/icelake-spark:latst
docker login ghcr.io
docker push ghcr.io/icelake-io/icelake-spark:0.1
docker push ghcr.io/icelake-io/icelake-spark:latest
