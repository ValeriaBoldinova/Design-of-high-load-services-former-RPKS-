-- Level 3: Pet types (deepest normalization)
CREATE TABLE dim_pet_category (
    pet_cat_id      SERIAL PRIMARY KEY,
    pet_cat_name    VARCHAR(100) NOT NULL UNIQUE
);

-- Level 2: Countries (shared across multiple dimensions)
CREATE TABLE dim_country (
    country_id      SERIAL PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL UNIQUE
);

-- Level 2: Product categories with link to pet category
CREATE TABLE dim_prod_category (
    prod_cat_id     SERIAL PRIMARY KEY,
    prod_cat_name   VARCHAR(100) NOT NULL UNIQUE,
    pet_cat_id      INTEGER REFERENCES dim_pet_category(pet_cat_id)
);

-- Level 2: Brands
CREATE TABLE dim_brand (
    brand_id        SERIAL PRIMARY KEY,
    brand_name      VARCHAR(100) NOT NULL UNIQUE
);

-- Level 1: Suppliers (references country)
CREATE TABLE dim_supplier (
    supplier_id     SERIAL PRIMARY KEY,
    supplier_name   VARCHAR(200),
    contact_person  VARCHAR(200),
    email_address   VARCHAR(200),
    phone_number    VARCHAR(50),
    address_line    VARCHAR(300),
    city_name       VARCHAR(100),
    country_id      INTEGER REFERENCES dim_country(country_id)
);

-- Level 1: Customers (references country)
CREATE TABLE dim_customer (
    cust_id         INTEGER PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    age_value       INTEGER,
    email_address   VARCHAR(200),
    country_id      INTEGER REFERENCES dim_country(country_id),
    postal_code     VARCHAR(20)
);

-- Level 2: Pets linked to customer
CREATE TABLE dim_pet (
    pet_id          SERIAL PRIMARY KEY,
    cust_id         INTEGER REFERENCES dim_customer(cust_id),
    pet_type_name   VARCHAR(50),
    pet_name        VARCHAR(100),
    pet_breed       VARCHAR(100)
);

-- Level 1: Sellers (references country)
CREATE TABLE dim_seller (
    seller_id       INTEGER PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email_address   VARCHAR(200),
    country_id      INTEGER REFERENCES dim_country(country_id),
    postal_code     VARCHAR(20)
);

-- Level 1: Products (references category, brand, supplier)
CREATE TABLE dim_product (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(200),
    prod_cat_id     INTEGER REFERENCES dim_prod_category(prod_cat_id),
    unit_price      NUMERIC(10,2),
    stock_qty       INTEGER,
    weight_kg       NUMERIC(10,2),
    color_name      VARCHAR(50),
    size_name       VARCHAR(50),
    brand_id        INTEGER REFERENCES dim_brand(brand_id),
    material_type   VARCHAR(100),
    product_desc    TEXT,
    avg_rating      NUMERIC(3,1),
    review_count    INTEGER,
    release_date    DATE,
    expiration_date DATE,
    supplier_id     INTEGER REFERENCES dim_supplier(supplier_id)
);

-- Level 1: Stores (references country)
CREATE TABLE dim_store (
    store_id        SERIAL PRIMARY KEY,
    store_name      VARCHAR(200),
    store_location  VARCHAR(200),
    city_name       VARCHAR(100),
    state_name      VARCHAR(100),
    country_id      INTEGER REFERENCES dim_country(country_id),
    phone_number    VARCHAR(50),
    email_address   VARCHAR(200)
);

-- Level 1: Dates
CREATE TABLE dim_date (
    date_key        SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    day_num         INTEGER,
    month_num       INTEGER,
    year_num        INTEGER,
    quarter_num     INTEGER,
    weekday_num     INTEGER
);

-- Fact table: Sales transactions
CREATE TABLE fact_sales (
    sale_id         SERIAL PRIMARY KEY,
    cust_id         INTEGER REFERENCES dim_customer(cust_id),
    seller_id       INTEGER REFERENCES dim_seller(seller_id),
    product_id      INTEGER REFERENCES dim_product(product_id),
    store_id        INTEGER REFERENCES dim_store(store_id),
    date_key        INTEGER REFERENCES dim_date(date_key),
    quantity_sold   INTEGER,
    total_price     NUMERIC(10,2)
);