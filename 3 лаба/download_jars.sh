#!/bin/bash
set -e

mkdir -p jars

JAR="jars/flink-sql-connector-kafka.jar"
URL="https://repo1.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"

if [ -f "$JAR" ]; then
    echo "JAR already exists: $JAR"
else
    echo "Downloading Flink Kafka connector..."
    wget -q "$URL" -O "$JAR"
    echo "Downloaded: $JAR"
fi
