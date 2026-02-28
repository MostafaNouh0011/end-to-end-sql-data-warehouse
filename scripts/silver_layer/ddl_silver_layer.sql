/*****************************************************************************************
Script Name : ddl_silver_layer.sql
Layer       : Silver Layer (Cleansed & Standardized Data)
Description :
      Creates Silver layer tables used for cleaned, validated, and standardized data
      before transformation into the Gold layer.
******************************************************************************************/

-- Ensure Schema Exists
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

/*===========================================================
  CRM TABLES
===========================================================*/

-- Customer Info
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(100),
    cst_lastname        NVARCHAR(100),
    cst_marital_status  NVARCHAR(20),
    cst_gndr            NVARCHAR(10),
    cst_create_date     DATE,
    dwh_load_date       DATETIME2       DEFAULT SYSDATETIME()
);
GO

  
-- Product Info
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(200),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_load_date   DATETIME2       DEFAULT SYSDATETIME()
);
GO


-- Sales Details
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_load_date   DATETIME2       DEFAULT SYSDATETIME(),

    CONSTRAINT PK_sales PRIMARY KEY (sls_ord_num, sls_prd_key)
);
GO


/*===========================================================
  ERP TABLES
===========================================================*/

-- ERP Customer
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(10),
    dwh_load_date   DATETIME2       DEFAULT SYSDATETIME()
);
GO


-- ERP Location
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),
    cntry           NVARCHAR(100),
    dwh_load_date   DATETIME2       DEFAULT SYSDATETIME()
);
GO


-- ERP Product Category
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(100),
    subcat          NVARCHAR(100),
    maintenance     NVARCHAR(50),
    dwh_load_date   DATETIME2       DEFAULT SYSDATETIME()
);
GO
