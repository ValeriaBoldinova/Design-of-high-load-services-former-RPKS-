CREATE TABLE IF NOT EXISTS report1_product_sales
(
    product_id UInt32,
    name String,
    category String,
    rating Float32,
    reviews UInt32,
    total_qty_sold UInt32,
    total_revenue Float64,
    sales_rank UInt32
)
ENGINE = MergeTree()
ORDER BY product_id;


CREATE TABLE IF NOT EXISTS report2_customer_sales
(
    customer_id UInt32,
    customer_name String,
    country String,
    total_orders UInt32,
    total_amount Float64,
    avg_check Float64,
    purchase_rank UInt32
)
ENGINE = MergeTree()
ORDER BY customer_id;


CREATE TABLE IF NOT EXISTS report3_time_sales
(
    year UInt32,
    month UInt32,
    month_name String,
    monthly_revenue Float64,
    total_orders UInt32,
    avg_order_size Float64,
    yearly_revenue Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);


CREATE TABLE IF NOT EXISTS report4_store_sales
(
    store_id UInt32,
    store_name String,
    city String,
    country String,
    total_orders UInt32,
    total_revenue Float64,
    avg_check Float64,
    revenue_rank UInt32
)
ENGINE = MergeTree()
ORDER BY store_id;


CREATE TABLE IF NOT EXISTS report5_supplier_sales
(
    supplier_id UInt32,
    supplier_name String,
    supplier_country String,
    total_revenue Float64,
    avg_product_price Float64,
    total_orders UInt32,
    revenue_rank UInt32
)
ENGINE = MergeTree()
ORDER BY supplier_id;


CREATE TABLE IF NOT EXISTS report6_product_quality
(
    product_id UInt32,
    product_name String,
    category String,
    rating Float32,
    reviews UInt32,
    total_sales_qty UInt32,
    total_revenue Float64,
    rating_rank UInt32,
    reviews_rank UInt32
)
ENGINE = MergeTree()
ORDER BY product_id;