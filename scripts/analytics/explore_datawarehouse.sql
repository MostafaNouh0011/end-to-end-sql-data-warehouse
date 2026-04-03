-- =======================================================
-- Data Warehouse Exploration Script
-- Layer: Analytics
-- Description: Initial exploration and EDA of Gold layer tables
-- =======================================================


-- 1. Explore all objects in the database
SELECT * 
FROM INFORMATION_SCHEMA.TABLES


-- 2. Preview key tables 
SELECT TOP 10 * FROM gold.dim_customers;
SELECT TOP 10 * FROM gold.dim_products;
SELECT TOP 10 * FROM gold.fact_sales;


-- 3. Row counts (table sizes)
SELECT 'customers' AS table_name, COUNT(*) AS num_of_rows FROM gold.dim_customers
UNION ALL
SELECT 'products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'sales', COUNT(*) FROM gold.fact_sales;


-- 4. Date range in sales
SELECT 
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order
FROM gold.fact_sales;


-- 5. Basic revenue stats
SELECT 
    MIN(sales_amount) AS min_revenue,
    MAX(sales_amount) AS max_revenue,
    AVG(sales_amount) AS avg_revenue
FROM gold.fact_sales;


-- 6. Top 10 customers by revenue
SELECT TOP 10
    customer_key,
    SUM(sales_amount) AS total_revenue
FROM gold.fact_sales
GROUP BY customer_key
ORDER BY total_revenue DESC;


-- 7. Top 10 products
SELECT TOP 10
    product_key,
    SUM(sales_amount) AS total_revenue
FROM gold.fact_sales
GROUP BY product_key
ORDER BY total_revenue DESC;


-- 8. Check duplicates (example)
SELECT 
    customer_key,
    COUNT(*) 
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
