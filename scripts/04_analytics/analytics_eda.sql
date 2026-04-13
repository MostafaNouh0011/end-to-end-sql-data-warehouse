/*
===============================================================================
Script: Exploratory Data Analysis (EDA)
Layer : Analytics

Description:
    This script performs exploratory data analysis on the Gold layer
    to understand data distribution, quality, and key business insights.

    Covers:
    - Data profiling
    - Data quality checks
    - Dimension exploration
    - Measures & KPIs
    - Magnitude & segmentation analysis
    - Ranking analysis
    - Time-based trends
===============================================================================
*/

USE DataWarehouse;

-------------------------------------------------------
-- SECTION 01 — DATA PROFILING
-------------------------------------------------------

-- List all tables in the database
SELECT * 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold'; 

-- Preview tables
SELECT TOP 10 * FROM gold.dim_customers;
SELECT TOP 10 * FROM gold.dim_products;
SELECT TOP 10 * FROM gold.fact_sales;

-- Row counts
SELECT 'dim_customers' AS table_name, COUNT(*)  AS row_count FROM gold.dim_customers
UNION ALL
SELECT 'dim_products',                COUNT(*)               FROM gold.dim_products
UNION ALL
SELECT 'fact_sales',                  COUNT(*)               FROM gold.fact_sales;


-------------------------------------------------------
-- SECTION 02 — DATA QUALITY CHECKS
-------------------------------------------------------

-- NULL checks
SELECT COUNT(*) AS null_order_dates
FROM gold.fact_sales
WHERE order_date IS NULL;

-- Duplicate customers
SELECT customer_key, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Negative or zero sales
SELECT *
FROM gold.fact_sales
WHERE sales_amount <= 0 OR quantity <= 0;


-------------------------------------------------------
-- SECTION 03 — DIMENSION EXPLORATION
-------------------------------------------------------

-- Unique countries
SELECT DISTINCT country 
FROM gold.dim_customers;

-- Product categories & subcategories
SELECT DISTINCT category, subcategory
FROM gold.dim_products;


-------------------------------------------------------
-- SECTION 04 — DATE EXPLORATION
-------------------------------------------------------

-- Sales date range
SELECT 
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS total_years
FROM gold.fact_sales;

-- Customer age distribution
SELECT
	MIN(birthdate)                            AS oldest_customer_birthdate,
	DATEDIFF(YEAR, MIN(birthdate), GETDATE()) AS oldest_customer_age,
	MAX(birthdate)                            AS youngest_customer_birthdate, 
	DATEDIFF(YEAR, MAX(birthdate), GETDATE()) AS youngest_customer_age
FROM gold.dim_customers


-------------------------------------------------------
-- SECTION 05 — KEY BUSINESS METRICS
-------------------------------------------------------

SELECT 'Total Revenue' AS metric, SUM(sales_amount) AS metric_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Avg Order Value', AVG(sales_amount) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Customers', COUNT(*) FROM gold.dim_customers
UNION ALL
SELECT 'Total Products', COUNT(*) FROM gold.dim_products;


-------------------------------------------------------
-- SECTION 06 — MAGNITUDE ANALYSIS
-------------------------------------------------------

-- Customers by country
SELECT country, COUNT(*) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- Find total customers by gender
SELECT 
	gender, 
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC

-- Products by category
SELECT category, COUNT(*) AS total_products
FROM gold.dim_products
GROUP BY category;

-- Revenue by category
SELECT 
    p.category,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
JOIN gold.dim_products p 
    ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;


-------------------------------------------------------
-- SECTION 07 — TIME-BASED ANALYSIS
-------------------------------------------------------

-- Sales trend by year
SELECT 
    YEAR(order_date) AS year,
    SUM(sales_amount) AS revenue
FROM gold.fact_sales
GROUP BY YEAR(order_date)
ORDER BY year;

-- Monthly trend
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(sales_amount) AS revenue
FROM gold.fact_sales
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-------------------------------------------------------
-- SECTION 08 — CUSTOMER ANALYSIS
-------------------------------------------------------

-- Top 10 customers generate the highest revenue
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount) AS total_spent
FROM gold.fact_sales s
JOIN gold.dim_customers c 
    ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_spent DESC;


-------------------------------------------------------
-- SECTION 09 — SEGMENTATION ANALYSIS
-------------------------------------------------------

WITH customer_spending AS (
    SELECT 
        customer_key,
        SUM(sales_amount) AS total_spent
    FROM gold.fact_sales
    GROUP BY customer_key
)
SELECT 
    CASE 
        WHEN total_spent > 10000 THEN 'High Value'
        WHEN total_spent > 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS segment,
    COUNT(*) AS customers
FROM customer_spending
GROUP BY 
    CASE 
        WHEN total_spent > 10000 THEN 'High Value'
        WHEN total_spent > 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END;


-------------------------------------------------------
-- SECTION 10 — RANKING ANALYSIS
-------------------------------------------------------

-- Which top 5 products generate the highest revenue?
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS revenue
FROM gold.fact_sales s
JOIN gold.dim_products p 
    ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY revenue DESC;

-- What are the 5 worst-performing products in terms of sales?
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS revenue
FROM gold.fact_sales s
JOIN gold.dim_products p 
    ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY revenue ASC;
