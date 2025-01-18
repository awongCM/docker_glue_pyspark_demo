#!/bin/bash
set -ex  # Exit on error, print commands

docker build .
docker-compose up -d
docker exec -it glue-pyspark-poc sh