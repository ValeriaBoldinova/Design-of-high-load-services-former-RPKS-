import json
import logging
import os
from datetime import datetime

import psycopg2

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction

from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "lab3_kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "pet_store_sales")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "lab3")
PG_USER = os.getenv("PG_USER", "flink")
PG_PASSWORD = os.getenv("PG_PASSWORD", "flink")


def _dsn():
    return (
        f"host={PG_HOST} "
        f"port={PG_PORT} "
        f"dbname={PG_DB} "
        f"user={PG_USER} "
        f"password={PG_PASSWORD}"
    )


def _int(v):
    try:
        return int(v) if v is not None and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _float(v):
    try:
        return float(v) if v is not None and str(v).strip() else None
    except (ValueError, TypeError):
        return None


def _parse_date(s):
    if not s:
        return None

    s = str(s).strip()

    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    return None


class StarSchemaSink(MapFunction):

    def __init__(self):
        self._conn = None
        self._cur = None
        self._count = 0

    def open_connection(self):

        if self._conn is None:

            self._conn = psycopg2.connect(_dsn())
            self._conn.autocommit = False

            self._cur = self._conn.cursor()

            log.info("Connected to PostgreSQL")

    def map(self, value):

        self.open_connection()

        try:

            row = json.loads(value)

            self._write_row(row)

            self._conn.commit()

            self._count += 1

            if self._count % 500 == 0:
                log.info("Processed %d records", self._count)

        except Exception as exc:

            log.error("Failed to process record: %s", exc)

            try:
                self._conn.rollback()
            except Exception:
                pass

        return value

    def _write_row(self, r):

        cur = self._cur


        cur.execute(
            """
            INSERT INTO dim_customer
            (
                customer_id,
                first_name,
                last_name,
                age,
                email,
                country,
                postal_code,
                pet_type,
                pet_name,
                pet_breed
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            (
                _int(r.get("sale_customer_id")),
                r.get("customer_first_name"),
                r.get("customer_last_name"),
                _int(r.get("customer_age")),
                r.get("customer_email"),
                r.get("customer_country"),
                r.get("customer_postal_code"),
                r.get("customer_pet_type"),
                r.get("customer_pet_name"),
                r.get("customer_pet_breed"),
            ),
        )


        cur.execute(
            """
            INSERT INTO dim_seller
            (
                seller_id,
                first_name,
                last_name,
                email,
                country,
                postal_code
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (seller_id) DO NOTHING
            """,
            (
                _int(r.get("sale_seller_id")),
                r.get("seller_first_name"),
                r.get("seller_last_name"),
                r.get("seller_email"),
                r.get("seller_country"),
                r.get("seller_postal_code"),
            ),
        )

 

        cur.execute(
            """
            INSERT INTO dim_product
            (
                product_id,
                name,
                category,
                pet_category,
                price,
                weight,
                color,
                size,
                brand,
                material,
                description,
                rating,
                reviews,
                release_date,
                expiry_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (product_id) DO NOTHING
            """,
            (
                _int(r.get("sale_product_id")),
                r.get("product_name"),
                r.get("product_category"),
                r.get("pet_category"),
                _float(r.get("product_price")),
                _float(r.get("product_weight")),
                r.get("product_color"),
                r.get("product_size"),
                r.get("product_brand"),
                r.get("product_material"),
                r.get("product_description"),
                _float(r.get("product_rating")),
                _int(r.get("product_reviews")),
                r.get("product_release_date"),
                r.get("product_expiry_date"),
            ),
        )


        store_name = r.get("store_name")

        if store_name:

            cur.execute(
                """
                INSERT INTO dim_store
                (
                    name,
                    location,
                    city,
                    state,
                    country,
                    phone,
                    email
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (name) DO NOTHING
                """,
                (
                    store_name,
                    r.get("store_location"),
                    r.get("store_city"),
                    r.get("store_state"),
                    r.get("store_country"),
                    r.get("store_phone"),
                    r.get("store_email"),
                ),
            )


        supplier_name = r.get("supplier_name")

        if supplier_name:

            cur.execute(
                """
                INSERT INTO dim_supplier
                (
                    name,
                    contact,
                    email,
                    phone,
                    address,
                    city,
                    country
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (name) DO NOTHING
                """,
                (
                    supplier_name,
                    r.get("supplier_contact"),
                    r.get("supplier_email"),
                    r.get("supplier_phone"),
                    r.get("supplier_address"),
                    r.get("supplier_city"),
                    r.get("supplier_country"),
                ),
            )


        sale_dt = _parse_date(r.get("sale_date"))

        date_id = None

        if sale_dt:

            date_id = int(sale_dt.strftime("%Y%m%d"))

            cur.execute(
                """
                INSERT INTO dim_date
                (
                    date_id,
                    full_date,
                    day,
                    month,
                    year,
                    quarter,
                    day_of_week,
                    month_name
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date_id) DO NOTHING
                """,
                (
                    date_id,
                    sale_dt.date(),
                    sale_dt.day,
                    sale_dt.month,
                    sale_dt.year,
                    (sale_dt.month - 1) // 3 + 1,
                    sale_dt.isoweekday(),
                    sale_dt.strftime("%B"),
                ),
            )


        store_id = None

        if store_name:

            cur.execute(
                "SELECT store_id FROM dim_store WHERE name = %s",
                (store_name,)
            )

            row = cur.fetchone()

            if row:
                store_id = row[0]


        supplier_id = None

        if supplier_name:

            cur.execute(
                "SELECT supplier_id FROM dim_supplier WHERE name = %s",
                (supplier_name,)
            )

            row = cur.fetchone()

            if row:
                supplier_id = row[0]


        cur.execute(
            """
            INSERT INTO fact_sales
            (
                customer_id,
                seller_id,
                product_id,
                store_id,
                supplier_id,
                date_id,
                quantity,
                total_price
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                _int(r.get("sale_customer_id")),
                _int(r.get("sale_seller_id")),
                _int(r.get("sale_product_id")),
                store_id,
                supplier_id,
                date_id,
                _int(r.get("sale_quantity")),
                _float(r.get("sale_total_price")),
            ),
        )


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink_star_schema")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    stream = stream.map(StarSchemaSink())

    log.info(
        "Flink job started. Reading topic '%s' from %s",
        TOPIC,
        KAFKA_BOOTSTRAP
    )

    env.execute("StarSchema_ETL")


if __name__ == "__main__":
    main()