-- Build Gold Layer


-- ============================================================================
-- Create Customer Dimension
-- ============================================================================

SELECT
    -- Customer Identifiers
    ci.cst_id            AS customer_id,
    ci.cst_key           AS customer_number,
  
    -- Customer Personal Information
    ci.cst_firstname     AS first_name,
    ci.cst_lastname      AS last_name,
    ca.bdate             AS birthdate,

    -- Customer Demographics
    la.cntry             AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender Resolution Logic
    CASE
        WHEN ci.cst_gndr = 'n/a'
            THEN COALESCE(ca.gen, 'n/a')
        ELSE ci.cst_gndr                   -- CRM is the master source for gender
    END AS gender,

    -- Metadata
    ci.cst_create_date   AS create_date

FROM silver.crm_cust_info      AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
       ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101  AS la
       ON ci.cst_key = la.cid;
