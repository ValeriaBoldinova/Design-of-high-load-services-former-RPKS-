INSERT INTO dim_pet_category (pet_cat_name)
SELECT DISTINCT TRIM(pet_cat_name)
FROM staging_mock_data
WHERE pet_cat_name IS NOT NULL AND TRIM(pet_cat_name) != ''
ORDER BY 1;

INSERT INTO dim_country (country_name)
SELECT DISTINCT country_name
FROM (
    SELECT NULLIF(TRIM(cust_country), '')  AS country_name FROM staging_mock_data
    UNION
    SELECT NULLIF(TRIM(seller_country), '')    FROM staging_mock_data
    UNION
    SELECT NULLIF(TRIM(store_country), '')     FROM staging_mock_data
    UNION
    SELECT NULLIF(TRIM(supplier_country_name), '')  FROM staging_mock_data
) t
WHERE country_name IS NOT NULL
ORDER BY 1;

INSERT INTO dim_prod_category (prod_cat_name, pet_cat_id)
SELECT DISTINCT ON (m.prod_category)
    TRIM(m.prod_category),
    pc.pet_cat_id
FROM staging_mock_data m
JOIN dim_pet_category pc ON pc.pet_cat_name = TRIM(m.pet_cat_name)
WHERE m.prod_category IS NOT NULL AND TRIM(m.prod_category) != ''
ORDER BY m.prod_category;

INSERT INTO dim_brand (brand_name)
SELECT DISTINCT TRIM(prod_brand)
FROM staging_mock_data
WHERE prod_brand IS NOT NULL AND TRIM(prod_brand) != ''
ORDER BY 1;

INSERT INTO dim_supplier (supplier_name, contact_person, email_address, phone_number, address_line, city_name, country_id)
SELECT DISTINCT ON (m.supplier_name, m.supplier_city_name)
    TRIM(m.supplier_name),
    m.supplier_contact_person,
    m.supplier_email,
    m.supplier_phone,
    m.supplier_address_line,
    m.supplier_city_name,
    c.country_id
FROM staging_mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.supplier_country_name), '')
WHERE m.supplier_name IS NOT NULL AND TRIM(m.supplier_name) != ''
ORDER BY m.supplier_name, m.supplier_city_name;

INSERT INTO dim_customer (cust_id, first_name, last_name, age_value, email_address, country_id, postal_code)
SELECT DISTINCT ON (m.sale_cust_id)
    m.sale_cust_id,
    m.cust_first_name,
    m.cust_last_name,
    m.cust_age,
    m.cust_email,
    c.country_id,
    m.cust_postal_code
FROM staging_mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.cust_country), '')
WHERE m.sale_cust_id IS NOT NULL
ORDER BY m.sale_cust_id;

INSERT INTO dim_pet (cust_id, pet_type_name, pet_name, pet_breed)
SELECT DISTINCT
    m.sale_cust_id,
    m.cust_pet_type,
    m.cust_pet_name,
    m.cust_pet_breed
FROM staging_mock_data m
WHERE m.sale_cust_id IS NOT NULL
  AND m.cust_pet_type IS NOT NULL
  AND TRIM(m.cust_pet_type) != '';

INSERT INTO dim_seller (seller_id, first_name, last_name, email_address, country_id, postal_code)
SELECT DISTINCT ON (m.sale_seller_id)
    m.sale_seller_id,
    m.seller_first_name,
    m.seller_last_name,
    m.seller_email,
    c.country_id,
    m.seller_postal_code
FROM staging_mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.seller_country), '')
WHERE m.sale_seller_id IS NOT NULL
ORDER BY m.sale_seller_id;

INSERT INTO dim_product (
    product_id, product_name, prod_cat_id, unit_price, stock_qty,
    weight_kg, color_name, size_name, brand_id, material_type, product_desc,
    avg_rating, review_count, release_date, expiration_date, supplier_id
)
SELECT DISTINCT ON (m.sale_prod_id)
    m.sale_prod_id,
    m.prod_name,
    pc.prod_cat_id,
    m.prod_price,
    m.prod_quantity,
    m.prod_weight,
    m.prod_color,
    m.prod_size,
    b.brand_id,
    m.prod_material,
    m.prod_desc,
    m.prod_rating,
    m.prod_review_cnt,
    CASE WHEN m.prod_release_date_str IS NOT NULL AND TRIM(m.prod_release_date_str) != ''
         THEN TO_DATE(m.prod_release_date_str, 'MM/DD/YYYY') ELSE NULL END,
    CASE WHEN m.prod_expiry_date_str IS NOT NULL AND TRIM(m.prod_expiry_date_str) != ''
         THEN TO_DATE(m.prod_expiry_date_str, 'MM/DD/YYYY') ELSE NULL END,
    s.supplier_id
FROM staging_mock_data m
LEFT JOIN dim_prod_category pc ON pc.prod_cat_name = TRIM(m.prod_category)
LEFT JOIN dim_brand b ON b.brand_name = TRIM(m.prod_brand)
LEFT JOIN dim_supplier s ON s.supplier_name = TRIM(m.supplier_name)
    AND TRIM(LOWER(COALESCE(s.city_name, ''))) = TRIM(LOWER(COALESCE(m.supplier_city_name, '')))
WHERE m.sale_prod_id IS NOT NULL
ORDER BY m.sale_prod_id;

INSERT INTO dim_store (store_name, store_location, city_name, state_name, country_id, phone_number, email_address)
SELECT DISTINCT ON (TRIM(m.store_name), TRIM(m.store_city))
    TRIM(m.store_name),
    m.store_location,
    TRIM(m.store_city),
    m.store_state,
    c.country_id,
    m.store_phone,
    m.store_email
FROM staging_mock_data m
LEFT JOIN dim_country c ON c.country_name = NULLIF(TRIM(m.store_country), '')
WHERE m.store_name IS NOT NULL AND TRIM(m.store_name) != ''
ORDER BY m.store_name, m.store_city;

INSERT INTO dim_date (full_date, day_num, month_num, year_num, quarter_num, weekday_num)
SELECT DISTINCT
    TO_DATE(sale_date_str, 'MM/DD/YYYY'),
    EXTRACT(DAY     FROM TO_DATE(sale_date_str, 'MM/DD/YYYY'))::INTEGER,
    EXTRACT(MONTH   FROM TO_DATE(sale_date_str, 'MM/DD/YYYY'))::INTEGER,
    EXTRACT(YEAR    FROM TO_DATE(sale_date_str, 'MM/DD/YYYY'))::INTEGER,
    EXTRACT(QUARTER FROM TO_DATE(sale_date_str, 'MM/DD/YYYY'))::INTEGER,
    EXTRACT(DOW     FROM TO_DATE(sale_date_str, 'MM/DD/YYYY'))::INTEGER
FROM staging_mock_data
WHERE sale_date_str IS NOT NULL AND TRIM(sale_date_str) != ''
ORDER BY 1;

INSERT INTO fact_sales (cust_id, seller_id, product_id, store_id, date_key, quantity_sold, total_price)
SELECT
    m.sale_cust_id,
    m.sale_seller_id,
    m.sale_prod_id,
    st.store_id,
    d.date_key,
    m.sale_qty,
    m.sale_total
FROM staging_mock_data m
LEFT JOIN dim_store st ON TRIM(LOWER(st.store_name)) = TRIM(LOWER(m.store_name))
    AND TRIM(LOWER(COALESCE(st.city_name, ''))) = TRIM(LOWER(COALESCE(m.store_city, '')))
LEFT JOIN dim_date d ON d.full_date = TO_DATE(m.sale_date_str, 'MM/DD/YYYY')
WHERE m.sale_cust_id IS NOT NULL
  AND m.sale_seller_id IS NOT NULL
  AND m.sale_prod_id IS NOT NULL;