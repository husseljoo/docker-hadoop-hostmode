#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <connector_type> (source-hdfs or destination-hdfs)"
  exit 1
fi

connector_type=$1

case "$connector_type" in
  "destination-hdfs")
    echo "Building destination-hdfs connector..."
    airbyte-ci connectors --name destination-hdfs build
    ;;
  "source-hdfs")
    echo "Building source-hdfs connector..."
    airbyte-ci connectors --name source-hdfs build
    ;;
  *)
    echo "Invalid connector type. Supported types: destination-hdfs, source-hdfs"
    exit 1
    ;;
esac

exit 0

