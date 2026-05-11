#!/bin/bash

echo "=== 1. ROW COUNTS ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT table_name, count FROM (
    SELECT 'dim_customer' AS table_name, COUNT(*) AS count FROM dim_customer
    UNION ALL SELECT 'dim_seller',  COUNT(*) FROM dim_seller
    UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
    UNION ALL SELECT 'dim_store',   COUNT(*) FROM dim_store
    UNION ALL SELECT 'dim_supplier',COUNT(*) FROM dim_supplier
    UNION ALL SELECT 'dim_date',    COUNT(*) FROM dim_date
    UNION ALL SELECT 'fact_sales',  COUNT(*) FROM fact_sales
) t ORDER BY table_name;"

echo ""
echo "=== 2. SALES BY MONTH ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT d.month_name, d.month, COUNT(*) AS sales,
       ROUND(SUM(f.total_price)::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.month, d.month_name
ORDER BY d.month;"

echo ""
echo "=== 3. TOP 3 CUSTOMERS BY TOTAL SPEND ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT c.first_name || ' ' || c.last_name AS customer,
       c.country,
       COUNT(f.sale_id) AS orders,
       ROUND(SUM(f.total_price)::numeric, 2) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.country
ORDER BY total_spent DESC
LIMIT 3;"

echo ""
echo "=== 4. TOP 5 STORES BY REVENUE ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT s.name AS store, s.city, s.country,
       COUNT(f.sale_id) AS sales,
       ROUND(SUM(f.total_price)::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.store_id, s.name, s.city, s.country
ORDER BY revenue DESC
LIMIT 5;"

echo ""
echo "=== 5. AVG ORDER VALUE BY PRODUCT CATEGORY ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT p.category,
       COUNT(*) AS sales,
       ROUND(AVG(f.total_price)::numeric, 2) AS avg_order_value,
       ROUND(MIN(f.total_price)::numeric, 2) AS min,
       ROUND(MAX(f.total_price)::numeric, 2) AS max
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY avg_order_value DESC;"

echo ""
echo "=== 6. SALES BY PET TYPE ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT c.pet_type,
       COUNT(*) AS sales,
       ROUND(SUM(f.total_price)::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.pet_type
ORDER BY sales DESC;"

echo ""
echo "=== 7. FK INTEGRITY (all should be 0) ==="
docker exec lab3_postgres psql -U flink -d lab3 -c "
SELECT
    (SELECT COUNT(*) FROM fact_sales WHERE customer_id NOT IN (SELECT customer_id FROM dim_customer)) AS missing_customers,
    (SELECT COUNT(*) FROM fact_sales WHERE seller_id   NOT IN (SELECT seller_id   FROM dim_seller))   AS missing_sellers,
    (SELECT COUNT(*) FROM fact_sales WHERE product_id  NOT IN (SELECT product_id  FROM dim_product))  AS missing_products,
    (SELECT COUNT(*) FROM fact_sales WHERE store_id    IS NULL) AS null_stores,
    (SELECT COUNT(*) FROM fact_sales WHERE supplier_id IS NULL) AS null_suppliers,
    (SELECT COUNT(*) FROM fact_sales WHERE date_id     IS NULL) AS null_dates;"
