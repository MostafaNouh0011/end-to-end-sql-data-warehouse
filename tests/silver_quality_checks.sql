/*
============================================================
Script : Silver Quality Checks
Purpose: Validate transformations performed in Silver Layer

Checks Included:
    1. Duplicate validation
    2. Null key checks
    3. Data standardization and consistency
    4. Date validation checks
    5. Sales calculation validation
    6. Data normalization validation
    7. Referential integrity checks

How This Is Used in The Data Warehouse:
                                                       Bronze Layer
                                                            │
                                                            ▼
                                                    Silver ETL Procedure
                                                            │
                                                            ▼
                                               Data Quality Checks (this script)
                                                            │
                                                            ▼
                                                        Gold Layer

>> Also, We can use it before making any transformations to detect the quality issues.
============================================================
*/

PRINT '=========================================';
PRINT 'Running Silver Layer Data Quality Checks';
PRINT '=========================================';


/*=========================================================
1. CRM CUSTOMER INFO CHECKS
=========================================================*/

PRINT '---- Checking crm_cust_info ----';

-- Duplicate Customers
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;


-- Null Customer IDs
-- Expectation: No Results
SELECT *
FROM silver.crm_cust_info
WHERE cst_id IS NULL;


-- Data Standardization & Consistency
-- Expectation: No Results
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single','Married','n/a');


-- Invalid Gender
-- Expectation: No Results
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Male','Female','n/a');


-- Leading or trailing spaces check
-- Expectation: No Results
SELECT *
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)
OR cst_lastname <> TRIM(cst_lastname);



/*=========================================================
2. PRODUCT INFO CHECKS
=========================================================*/

PRINT '---- Checking crm_prd_info ----';

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Invalid Product Line
-- Expectation: No Results
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain','Road','Touring','Other Sales','n/a');


-- Negative Product Cost
-- Expectation: No Results
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost < 0;


-- Date validity check (Start Date > End Date)
-- Expectation: No Results
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;



/*=========================================================
3. SALES DETAILS CHECKS
=========================================================*/

PRINT '---- Checking crm_sales_details ----';

-- Null Order Numbers
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL;


-- Sales mismatch validation
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * ABS(sls_price);


-- Negative or zero quantity
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity <= 0;


-- Date consistency check
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_ship_dt < sls_order_dt
OR sls_due_dt < sls_order_dt;



/*=========================================================
4. ERP CUSTOMER CHECKS
=========================================================*/

PRINT '---- Checking erp_cust_az12 ----';

-- Check future birthdates
-- Expectation: No Results
SELECT *
FROM silver.erp_cust_az12
WHERE bdate > GETDATE();


-- Data Standardization & Consistency
SELECT 
    DISTINCT gen
FROM silver.erp_cust_az12;


-- Null Customer IDs
-- Expectation: No Results
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL;



/*=========================================================
5. ERP LOCATION CHECKS
=========================================================*/

PRINT '---- Checking erp_loc_a101 ----';

-- Null Customer IDs
-- Expectation: No Results
SELECT *
FROM silver.erp_loc_a101
WHERE cid IS NULL;


-- Country normalization validation
-- Expectation: No Results
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
WHERE cntry IS NULL;



/*=========================================================
6. ERP PRODUCT CATEGORY CHECKS
=========================================================*/

PRINT '---- Checking erp_px_cat_g1v2 ----';

-- Null IDs 
-- Expectation: No Results
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;


-- Maintenance column contains line breaks
-- Expectation: No Results
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE maintenance LIKE '%' + CHAR(10) + '%'
OR maintenance LIKE '%' + CHAR(13) + '%';


-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;

/*=========================================================
7. CROSS TABLE REFERENTIAL CHECKS
=========================================================*/

PRINT '---- Referential Integrity Checks ----';

-- Sales referencing non-existing customers
SELECT s.*
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c
ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;


-- Sales referencing non-existing products
SELECT s.*
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;


PRINT '=========================================';
PRINT 'Silver Layer Quality Checks Completed';
PRINT '=========================================';
