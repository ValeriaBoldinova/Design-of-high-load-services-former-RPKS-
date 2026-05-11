from pyspark.sql import SparkSession

PG_URL = "jdbc:postgresql://postgres:5432/lab2"
CH_URL = "jdbc:clickhouse://clickhouse:8123/default"

PG_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

spark = (
    SparkSession.builder
    .appName("VERIFY_LAB2")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def read_pg(table):
    return (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .options(**PG_PROPS)
        .load()
    )


def read_ch(table):
    return (
        spark.read.format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", table)
        .options(**CH_PROPS)
        .load()
    )


def show(df, name, n=10):
    print(f"\n===== {name} =====")
    print("rows:", df.count())
    df.show(n, truncate=False)


print("\n================ VERIFY POSTGRES =================")

dim_customer = read_pg("dim_customer")
dim_product = read_pg("dim_product")
fact_sales = read_pg("fact_sales")
dim_date = read_pg("dim_date")

show(dim_customer, "dim_customer")
show(dim_product, "dim_product")
show(fact_sales, "fact_sales")
show(dim_date, "dim_date")

print("\nTOP products (Postgres):")
(
    fact_sales.join(dim_product, "product_id")
    .groupBy("name")
    .sum("quantity")
    .orderBy("sum(quantity)", ascending=False)
    .show(10, False)
)


print("\n================ VERIFY CLICKHOUSE =================")

tables = [
    "report1_product_sales",
    "report2_customer_sales",
    "report3_time_sales",
    "report4_store_sales",
    "report5_supplier_sales",
    "report6_product_quality",
]

for t in tables:
    try:
        df = read_ch(t)
        show(df, t)
    except Exception as e:
        print(f"[ERROR] {t}: {e}")

print("\n================ DONE =================")

spark.stop()