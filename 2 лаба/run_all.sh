#!/bin/bash

set -e

echo "== STOP OLD =="
docker compose down -v

echo "== START CONTAINERS =="
docker compose up -d

echo "== WAIT POSTGRES =="
sleep 20

SPARK_CONF="\
--conf spark.ui.showConsoleProgress=false \
--conf spark.sql.shuffle.partitions=4 \
--conf spark.driver.log.level=ERROR \
--conf spark.executor.log.level=ERROR \
"

SPARK_LOGGING="\
--conf spark.ui.showConsoleProgress=false \
--conf spark.eventLog.enabled=false \
--conf spark.sql.shuffle.partitions=4 \
--conf spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false \
--conf spark.log.level=ERROR \
"

export PYTHONWARNINGS="ignore"

echo "== STAR SCHEMA =="
docker exec lab2_spark /opt/spark/bin/spark-submit \
  $SPARK_CONF \
  --master local[*] \
  --jars /opt/spark-jars/postgresql-42.7.3.jar \
  /opt/spark-apps/1_etl_star_schema.py

echo "== CLICKHOUSE ETL =="
docker exec lab2_spark /opt/spark/bin/spark-submit \
  $SPARK_CONF \
  --master local[*] \
  --jars /opt/spark-jars/postgresql-42.7.3.jar,/opt/spark-jars/clickhouse-jdbc-0.6.3-all.jar \
  /opt/spark-apps/2_etl_clickhouse.py

echo "== VERIFY ==" 
docker exec lab2_spark /opt/spark/bin/spark-submit \
  $SPARK_CONF \
  --master local[*] \
  --jars /opt/spark-jars/postgresql-42.7.3.jar,/opt/spark-jars/clickhouse-jdbc-0.6.3-all.jar \
  /opt/spark-apps/verify_results.py

echo "== DONE =="