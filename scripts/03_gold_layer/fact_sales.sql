/*
====================================================================================
Gold Layer - Fact: Sales
====================================================================================
Description:
    Creates the Sales fact view by combining sales transactions with
    customer and product dimensions.
====================================================================================
*/

CREATE OR ALTER VIEW gold.fact_sales AS

SELECT

    -- Dimension Keys
    dc.customer_key,
    dp.product_key,

    -- Transaction Information
    sd.sls_ord_num       AS order_number,
    sd.sls_order_dt      AS order_date,
    sd.sls_ship_dt       AS ship_date,
    sd.sls_due_dt        AS due_date,

    -- Measures
    sd.sls_sales         AS sales_amount,
    sd.sls_quantity      AS quantity,
    sd.sls_price         AS price

FROM silver.crm_sales_details sd

LEFT JOIN gold.dim_customers dc
       ON sd.sls_cust_id = dc.customer_id
  
LEFT JOIN gold.dim_products dp
       ON sd.sls_prd_key = dp.product_number;
