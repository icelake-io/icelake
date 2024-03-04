#!/bin/bash

set -ex

docker build --network host -t icelake-spark .
docker tag icelake-spark:latest ghcr.io/icelake-io/icelake-spark:latest
docker login ghcr.io
docker push ghcr.io/icelake-io/icelake-spark:latest