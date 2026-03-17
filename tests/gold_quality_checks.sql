/*
====================================================================================
Script Name : Gold Quality Checks
Layer       : Gold Layer

Description:
    This script performs data quality checks on the Gold layer tables:
    - gold.dim_customers
    - gold.dim_products
    - gold.fact_sales

Checks Included:
    • NULL value checks
    • Duplicate key checks
    • Referential integrity checks
    • Business rule validations
====================================================================================
*/

USE DataWarehouse;

PRINT '====================================================';
PRINT 'Gold Layer Data Quality Checks';
PRINT '====================================================';

-- ====================================================
-- 1. CHECK: NULL VALUES IN PRIMARY KEYS
-- ====================================================

PRINT 'Checking NULLs in Primary Keys...';

SELECT 'dim_customers' AS table_name, COUNT(*) AS null_count
FROM gold.dim_customers
WHERE customer_key IS NULL

UNION ALL

SELECT 'dim_products', COUNT(*)
FROM gold.dim_products
WHERE product_key IS NULL

UNION ALL

SELECT 'fact_sales', COUNT(*)
FROM gold.fact_sales
WHERE order_number IS NULL;

-- ====================================================
-- 2. CHECK: DUPLICATE PRIMARY KEYS
-- ====================================================

PRINT 'Checking Duplicate Keys...';

SELECT customer_key, COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

SELECT product_key, COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================
-- 3. CHECK: REFERENTIAL INTEGRITY
-- ====================================================

PRINT 'Checking Referential Integrity...';

-- Invalid customer_key
SELECT COUNT(*) AS invalid_customer_keys
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
    ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

-- Invalid product_key
SELECT COUNT(*) AS invalid_product_keys
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
    ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- ====================================================
-- 4. CHECK: NULL OR MISSING CRITICAL FIELDS
-- ====================================================

PRINT 'Checking Critical Columns...';

-- Customers
SELECT COUNT(*) AS missing_customer_names
FROM gold.dim_customers
WHERE first_name IS NULL OR last_name IS NULL;

-- Products
SELECT COUNT(*) AS missing_product_names
FROM gold.dim_products
WHERE product_name IS NULL;

-- Fact Sales
SELECT COUNT(*) AS missing_sales_values
FROM gold.fact_sales
WHERE sales_amount IS NULL OR quantity IS NULL;

-- ====================================================
-- 5. CHECK: BUSINESS RULE VALIDATIONS
-- ====================================================

PRINT 'Checking Business Rules...';

-- Negative or zero sales
SELECT COUNT(*) AS invalid_sales_amount
FROM gold.fact_sales
WHERE sales_amount <= 0;

-- Negative quantity
SELECT COUNT(*) AS invalid_quantity
FROM gold.fact_sales
WHERE quantity <= 0;

-- Invalid price
SELECT COUNT(*) AS invalid_price
FROM gold.fact_sales
WHERE price <= 0;

-- ====================================================
-- 6. CHECK: DATE CONSISTENCY
-- ====================================================

PRINT 'Checking Date Consistency...';

SELECT COUNT(*) AS invalid_dates
FROM gold.fact_sales
WHERE order_date > ship_date
   OR order_date > due_date;

-- ====================================================
-- 7. SUMMARY COUNTS
-- ====================================================

PRINT 'Summary Row Counts...';

SELECT 'dim_customers' AS table_name, COUNT(*) AS row_count
FROM gold.dim_customers

UNION ALL

SELECT 'dim_products', COUNT(*)
FROM gold.dim_products

UNION ALL

SELECT 'fact_sales', COUNT(*)
FROM gold.fact_sales;

PRINT '====================================================';
PRINT 'Data Quality Checks Completed';
PRINT '====================================================';
