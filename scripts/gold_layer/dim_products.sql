/*
====================================================================================
Gold Layer - Dimension: Products
====================================================================================
Description:
    Creates the Product dimension view by combining CRM product data
    with ERP category information.
====================================================================================
*/

CREATE OR ALTER VIEW gold.dim_products AS

SELECT

    -- Surrogate Key
    ROW_NUMBER() OVER (ORDER BY prd.prd_start_dt, prd.prd_key) AS product_key,

    -- Product Identifiers
    prd.prd_id        AS product_id,
    prd.prd_key       AS product_number,

    -- Product Information
    prd.prd_nm        AS product_name,
    prd.prd_line      AS product_line,

    -- Product Category
    prd.cat_id        AS category_id,
    cat.cat           AS category,
    cat.subcat        AS subcategory,
    cat.maintenance,

    -- Product Cost Information
    prd.prd_cost      AS cost,

    -- Product Metadata
    prd.prd_start_dt  AS start_date

FROM silver.crm_prd_info AS prd
LEFT JOIN silver.erp_px_cat_g1v2 AS cat
ON prd.cat_id = cat.id

WHERE prd.prd_end_dt IS NULL;       -- Only include active products
