"""
ETL: Star Schema (PostgreSQL) -> 6 отчётов (ClickHouse)
Таблицы создаются Spark'ом через JDBC (mode=overwrite).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/lab2"
PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

CH_URL    = "jdbc:clickhouse://clickhouse:8123/default"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
CH_ENGINE = "ENGINE = MergeTree() ORDER BY tuple()"

spark = (
    SparkSession.builder
    .appName("Lab2_ClickHouse")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def read_pg(table):
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .options(**PG_PROPS)
        .load()
    )


def write_ch(df, table):
    (
        df.write.format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", table)
        .option("driver", CH_DRIVER)
        .option("createTableOptions", CH_ENGINE)
        .mode("append")
        .save()
    )
    print(f"[OK] ClickHouse -> {table}")


# ── Загрузка звезды ───────────────────────────────────────────────────────────
fact         = read_pg("fact_sales")
dim_product  = read_pg("dim_product").cache()
dim_customer = read_pg("dim_customer").cache()
dim_store    = read_pg("dim_store").cache()
dim_supplier = read_pg("dim_supplier").cache()
dim_date     = read_pg("dim_date").cache()

# ─── Отчёт 1: Витрина продаж по продуктам ────────────────────────────────────
# Топ-10 по кол-ву продаж, выручка по категориям, средний рейтинг
r1 = (
    fact.join(dim_product, "product_id")
    .groupBy("product_id", "name", "category", "rating", "reviews")
    .agg(
        F.sum("quantity").alias("total_qty_sold"),
        F.round(F.sum("total_price"), 2).alias("total_revenue"),
    )
    .withColumn("sales_rank", F.rank().over(Window.orderBy(F.desc("total_qty_sold"))))
)
write_ch(r1, "report1_product_sales")

# ─── Отчёт 2: Витрина продаж по клиентам ─────────────────────────────────────
# Топ-10 по сумме, распределение по странам, средний чек
r2 = (
    fact.join(dim_customer, "customer_id")
    .groupBy(
        "customer_id",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
        F.col("country"),
    )
    .agg(
        F.count("sale_id").alias("total_orders"),
        F.round(F.sum("total_price"), 2).alias("total_amount"),
        F.round(F.avg("total_price"), 2).alias("avg_check"),
    )
    .withColumn("purchase_rank", F.rank().over(Window.orderBy(F.desc("total_amount"))))
)
write_ch(r2, "report2_customer_sales")

# ─── Отчёт 3: Витрина продаж по времени ──────────────────────────────────────
# Месячные и годовые тренды, средний размер заказа

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_time = fact.join(dim_date, "date_id")

r3 = (
    df_time
    .groupBy("year", "month", "month_name")
    .agg(
        F.round(F.sum("total_price"), 2).alias("monthly_revenue"),
        F.count("sale_id").alias("total_orders"),
        F.round(F.avg("quantity"), 2).alias("avg_order_size"),
    )
)

# окно для годовой выручки
w = Window.partitionBy("year")

r3 = (
    r3.withColumn(
        "yearly_revenue",
        F.round(F.sum("monthly_revenue").over(w), 2)
    )
    .orderBy("year", "month")
)

write_ch(r3, "report3_time_sales")

# ─── Отчёт 4: Витрина продаж по магазинам ────────────────────────────────────
# Топ-5 по выручке, распределение по городам
r4 = (
    fact.join(dim_store, "store_id")
    .groupBy("store_id", F.col("name").alias("store_name"), "city", "country")
    .agg(
        F.count("sale_id").alias("total_orders"),
        F.round(F.sum("total_price"), 2).alias("total_revenue"),
        F.round(F.avg("total_price"), 2).alias("avg_check"),
    )
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.desc("total_revenue"))))
)
write_ch(r4, "report4_store_sales")

# ─── Отчёт 5: Витрина продаж по поставщикам ──────────────────────────────────
# Топ-5 по выручке, средняя цена товаров, распределение по странам
r5_base = (
    fact
    .join(dim_supplier, "supplier_id")
    .join(
        dim_product.select("product_id", F.col("price").alias("product_price")),
        "product_id",
    )
)
r5 = (
    r5_base
    .groupBy("supplier_id", "name", "country")
    .agg(
        F.round(F.sum("total_price"), 2).alias("total_revenue"),
        F.round(F.avg("product_price"), 2).alias("avg_product_price"),
        F.count("sale_id").alias("total_orders"),
    )
    .withColumnRenamed("name", "supplier_name")
    .withColumnRenamed("country", "supplier_country")
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.desc("total_revenue"))))
)
write_ch(r5, "report5_supplier_sales")

# ─── Отчёт 6: Витрина качества продукции ─────────────────────────────────────
# Рейтинг, кол-во отзывов, корреляция рейтинга с продажами
r6 = (
    fact.join(dim_product, "product_id")
    .groupBy("product_id", F.col("name").alias("product_name"), "category", "rating", "reviews")
    .agg(
        F.sum("quantity").alias("total_sales_qty"),
        F.round(F.sum("total_price"), 2).alias("total_revenue"),
    )
    .withColumn("rating_rank",  F.rank().over(Window.orderBy(F.desc("rating"))))
    .withColumn("reviews_rank", F.rank().over(Window.orderBy(F.desc("reviews"))))
)
write_ch(r6, "report6_product_quality")

print("=== ETL ClickHouse завершён ===")
spark.stop()
