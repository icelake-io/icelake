#!/bin/bash

set -ex

docker build -t icelake-spark .
docker tag icelake-spark:latest icelake-spark:latest