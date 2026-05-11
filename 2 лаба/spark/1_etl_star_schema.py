"""
ETL: mock_data (PostgreSQL) -> Star Schema (PostgreSQL)
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

spark = (
    SparkSession.builder
    .appName("Lab2_StarSchema")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def write_pg(df, table):
    df.write.format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", table) \
        .options(**PG_PROPS) \
        .mode("overwrite") \
        .save()
    print(f"[OK] {table}")


# ── Чтение источника ──────────────────────────────────────────────────────────
df = (
    spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "mock_data")
    .options(**PG_PROPS)
    .load()
)

# Явное приведение типов (на случай если JDBC отдал строки)
df = (
    df
    .withColumn("customer_age",      F.col("customer_age").cast("int"))
    .withColumn("product_price",     F.col("product_price").cast("decimal(10,2)"))
    .withColumn("product_quantity",  F.col("product_quantity").cast("int"))
    .withColumn("product_weight",    F.col("product_weight").cast("decimal(10,2)"))
    .withColumn("product_rating",    F.col("product_rating").cast("decimal(3,1)"))
    .withColumn("product_reviews",   F.col("product_reviews").cast("int"))
    .withColumn("sale_customer_id",  F.col("sale_customer_id").cast("int"))
    .withColumn("sale_seller_id",    F.col("sale_seller_id").cast("int"))
    .withColumn("sale_product_id",   F.col("sale_product_id").cast("int"))
    .withColumn("sale_quantity",     F.col("sale_quantity").cast("int"))
    .withColumn("sale_total_price",  F.col("sale_total_price").cast("decimal(12,2)"))
)

df.cache()
print(f"Загружено строк: {df.count()}")

# ── dim_customer ──────────────────────────────────────────────────────────────
dim_customer = (
    df.select(
        F.col("sale_customer_id").alias("customer_id"),
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age").alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type").alias("pet_type"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed"),
    )
    .dropDuplicates(["customer_id"])
)
write_pg(dim_customer, "dim_customer")

# ── dim_seller ────────────────────────────────────────────────────────────────
dim_seller = (
    df.select(
        F.col("sale_seller_id").alias("seller_id"),
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .dropDuplicates(["seller_id"])
)
write_pg(dim_seller, "dim_seller")

# ── dim_product ───────────────────────────────────────────────────────────────
dim_product = (
    df.select(
        F.col("sale_product_id").alias("product_id"),
        F.col("product_name").alias("name"),
        F.col("product_category").alias("category"),
        F.col("pet_category"),
        F.col("product_price").alias("price"),
        F.col("product_weight").alias("weight"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_brand").alias("brand"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.col("product_rating").alias("rating"),
        F.col("product_reviews").alias("reviews"),
        F.col("product_release_date").alias("release_date"),
        F.col("product_expiry_date").alias("expiry_date"),
    )
    .dropDuplicates(["product_id"])
)
write_pg(dim_product, "dim_product")

# ── dim_store  (суррогатный ключ по имени магазина) ───────────────────────────
stores_dedup = (
    df.select(
        "store_name", "store_location", "store_city",
        "store_state", "store_country", "store_phone", "store_email",
    )
    .dropDuplicates(["store_name"])
    .na.fill({"store_name": "Unknown"})
)
w_store = Window.orderBy("store_name")
dim_store = (
    stores_dedup
    .withColumn("store_id", F.row_number().over(w_store))
    .select(
        F.col("store_id"),
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    )
)
write_pg(dim_store, "dim_store")

# ── dim_supplier  (суррогатный ключ по имени поставщика) ─────────────────────
suppliers_dedup = (
    df.select(
        "supplier_name", "supplier_contact", "supplier_email",
        "supplier_phone", "supplier_address", "supplier_city", "supplier_country",
    )
    .dropDuplicates(["supplier_name"])
    .na.fill({"supplier_name": "Unknown"})
)
w_supplier = Window.orderBy("supplier_name")
dim_supplier = (
    suppliers_dedup
    .withColumn("supplier_id", F.row_number().over(w_supplier))
    .select(
        F.col("supplier_id"),
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country"),
    )
)
write_pg(dim_supplier, "dim_supplier")

# ── dim_date ──────────────────────────────────────────────────────────────────
dim_date = (
    df.select(F.to_date(F.col("sale_date"), "M/d/yyyy").alias("full_date"))
    .filter(F.col("full_date").isNotNull())
    .dropDuplicates(["full_date"])
    .withColumn("date_id",     F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("day",         F.dayofmonth("full_date"))
    .withColumn("month",       F.month("full_date"))
    .withColumn("year",        F.year("full_date"))
    .withColumn("quarter",     F.quarter("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("month_name",  F.date_format("full_date", "MMMM"))
)
write_pg(dim_date, "dim_date")

# ── fact_sales ────────────────────────────────────────────────────────────────
# Получаем суррогатные ключи магазина и поставщика через join
store_lkp    = dim_store.select("store_id", F.col("name").alias("_sname"))
supplier_lkp = dim_supplier.select("supplier_id", F.col("name").alias("_supname"))

df_f = (
    df
    .join(store_lkp,    df.store_name    == F.col("_sname"),   "left")
    .join(supplier_lkp, df.supplier_name == F.col("_supname"), "left")
)

fact_sales = df_f.select(
    F.monotonically_increasing_id().alias("sale_id"),
    F.col("sale_customer_id").alias("customer_id"),
    F.col("sale_seller_id").alias("seller_id"),
    F.col("sale_product_id").alias("product_id"),
    F.col("store_id"),
    F.col("supplier_id"),
    F.date_format(
        F.to_date(F.col("sale_date"), "M/d/yyyy"), "yyyyMMdd"
    ).cast("int").alias("date_id"),
    F.col("sale_quantity").alias("quantity"),
    F.col("sale_total_price").alias("total_price"),
)
write_pg(fact_sales, "fact_sales")

df.unpersist()
print("=== ETL Star Schema завершён ===")
spark.stop()
