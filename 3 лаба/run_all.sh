#!/bin/bash
set -e

echo "== DOWNLOAD JARS =="
./download_jars.sh

echo "== STOP OLD =="
docker compose down -v

echo "== BUILD IMAGES =="
docker compose build flink-job kafka-producer

echo "== START INFRASTRUCTURE =="
docker compose up -d postgres kafka

echo "== WAIT FOR POSTGRESQL =="
until [ "$(docker inspect --format='{{.State.Health.Status}}' lab3_postgres 2>/dev/null)" = "healthy" ]; do
    echo "  PostgreSQL not ready yet..."
    sleep 3
done
echo "  PostgreSQL is ready."

echo "== WAIT FOR KAFKA =="
until [ "$(docker inspect --format='{{.State.Health.Status}}' lab3_kafka 2>/dev/null)" = "healthy" ]; do
    echo "  Kafka not ready yet..."
    sleep 5
done
echo "  Kafka is ready."

echo "== CREATE KAFKA TOPIC =="
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --topic pet_store_sales \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists
echo "  Topic pet_store_sales created."

echo "== START FLINK JOB =="
docker compose up -d flink-job
echo "  Flink job started, waiting 20s for initialization..."
sleep 20

echo "== RUN KAFKA PRODUCER =="
docker compose run --rm kafka-producer

echo "== WAIT FOR FLINK TO PROCESS ALL RECORDS =="
echo "  Waiting 40s for Flink to flush remaining records..."
sleep 40

echo "== RESULTS IN POSTGRESQL =="
docker compose exec postgres psql -U flink -d lab3 -c "
SELECT table_name, count
FROM (
    SELECT 'dim_customer' AS table_name, COUNT(*) AS count FROM dim_customer
    UNION ALL SELECT 'dim_seller',  COUNT(*) FROM dim_seller
    UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
    UNION ALL SELECT 'dim_store',   COUNT(*) FROM dim_store
    UNION ALL SELECT 'dim_supplier',COUNT(*) FROM dim_supplier
    UNION ALL SELECT 'dim_date',    COUNT(*) FROM dim_date
    UNION ALL SELECT 'fact_sales',  COUNT(*) FROM fact_sales
) t
ORDER BY table_name;
"

echo ""
echo "== DONE =="
echo "Flink job is still running (streaming mode)."
echo "To stop all containers: docker compose down"
