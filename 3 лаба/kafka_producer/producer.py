import csv
import glob
import json
import logging
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "pet_store_sales"


def connect(max_retries=30, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.info("Kafka not ready, attempt %d/%d, retrying in %ds...", attempt, max_retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % max_retries)


def main():
    producer = connect()

    csv_files = sorted(glob.glob("/data/csv/*.csv"))
    log.info("Found %d CSV files", len(csv_files))

    total = 0
    for filepath in csv_files:
        log.info("Sending: %s", filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                producer.send(TOPIC, value=dict(row))
                total += 1

    producer.flush()
    log.info("Done. Total records sent to Kafka: %d", total)


if __name__ == "__main__":
    main()
