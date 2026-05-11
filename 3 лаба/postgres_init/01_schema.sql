CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id  INT PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    age          INT,
    email        VARCHAR(200),
    country      VARCHAR(100),
    postal_code  VARCHAR(20),
    pet_type     VARCHAR(50),
    pet_name     VARCHAR(100),
    pet_breed    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id   INT PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200),
    country     VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id   INT PRIMARY KEY,
    name         VARCHAR(200),
    category     VARCHAR(100),
    pet_category VARCHAR(100),
    price        DECIMAL(10,2),
    weight       DECIMAL(10,2),
    color        VARCHAR(50),
    size         VARCHAR(50),
    brand        VARCHAR(100),
    material     VARCHAR(100),
    description  TEXT,
    rating       DECIMAL(3,1),
    reviews      INT,
    release_date VARCHAR(20),
    expiry_date  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id  SERIAL PRIMARY KEY,
    name      VARCHAR(200) UNIQUE NOT NULL,
    location  VARCHAR(200),
    city      VARCHAR(100),
    state     VARCHAR(50),
    country   VARCHAR(100),
    phone     VARCHAR(50),
    email     VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name        VARCHAR(200) UNIQUE NOT NULL,
    contact     VARCHAR(100),
    email       VARCHAR(200),
    phone       VARCHAR(50),
    address     VARCHAR(200),
    city        VARCHAR(100),
    country     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INT PRIMARY KEY,
    full_date   DATE,
    day         INT,
    month       INT,
    year        INT,
    quarter     INT,
    day_of_week INT,
    month_name  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id     SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id   INT REFERENCES dim_seller(seller_id),
    product_id  INT REFERENCES dim_product(product_id),
    store_id    INT REFERENCES dim_store(store_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    date_id     INT REFERENCES dim_date(date_id),
    quantity    INT,
    total_price DECIMAL(12,2)
);
