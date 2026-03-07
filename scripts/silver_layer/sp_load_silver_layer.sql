/*
=============================================================
Stored Procedure : sp_load_silver_layer
Database         : DataWarehouse
Layer            : Silver Layer (Data Cleansing & Transformation)

Description:
	This stored procedure loads data from the Bronze layer into the Silver layer of the Data Warehouse.


During the load process, the script performs several data
cleaning and transformation steps including:

	• Removing duplicate records
	• Standardizing text values (TRIM, UPPER, formatting)
	• Normalizing categorical fields (gender, marital status, country)
	• Cleaning special characters and line breaks
	• Fixing or deriving missing numeric values
	• Validating and converting date fields
	• Calculating product validity periods
	• Handling null or incorrect values


Each Silver table is refreshed using (TRUNCATE + INSERT) to ensure only the latest cleaned data is stored.

>> Source : bronze schema tables
>> Target : silver schema tables
=============================================================*/


CREATE OR ALTER PROCEDURE silver.sp_load_silver_layer AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time      DATETIME2,
        @end_time        DATETIME2,
        @batch_start     DATETIME2,
        @batch_end       DATETIME2;

    BEGIN TRY
        BEGIN TRANSACTION;

        SET @batch_start = SYSDATETIME();

        PRINT '==============================================';
        PRINT 'Loading Silver Layer ..';
        PRINT '==============================================';

        /*====================================================
        Clean & Load crm_cust_info  (Truncate + Insert)
        ====================================================*/
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END,                                      -- Normalize marital status values to readable format 
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'M' THEN 'Male'
                WHEN 'F' THEN 'Female'
                ELSE 'n/a'
            END,                                      -- Normalize gender values to readable format
            cst_create_date
        FROM
        (
            SELECT *,
                   ROW_NUMBER() OVER
                   (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS rn
            FROM bronze.crm_cust_info
        ) t
        WHERE rn = 1
        AND cst_id IS NOT NULL;                       -- Select the most recent record by customer

        SET @end_time = SYSDATETIME();
        PRINT 'Load Duration for crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds';
        PRINT '-----------------------------';


        /*====================================================
        Clean & Load crm_prd_info  (Truncate + Insert)
        ====================================================*/
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info
        (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),  -- Extract Category ID
            SUBSTRING(prd_key,7,LEN(prd_key)),        -- Extract Product Key
            prd_nm,
            ISNULL(prd_cost,0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,                                      -- Map product line codes to descriptive values
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) OVER
                (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE                         -- Calculate end date as one day before the next start date
            )
        FROM bronze.crm_prd_info;

        SET @end_time = SYSDATETIME();
        PRINT 'Load Duration for crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds';
        PRINT '-----------------------------';


        /*====================================================
        Clean & Load crm_sales_details  (Truncate + Insert)
        ====================================================*/
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
           
            CASE 
	            WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL 
	            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,

            CASE 
	            WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL 
	            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,

            CASE 
	            WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL 
	            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,

            CASE
                WHEN sls_sales IS NULL
                     OR sls_sales <= 0
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,                                      -- Recalculate sales if original value is missing or incorrect

            sls_quantity,

            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END                                       -- Derive price if original value is invalid 
        FROM bronze.crm_sales_details;

        SET @end_time = SYSDATETIME();
        PRINT 'Load Duration for crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds';
        PRINT '-----------------------------';
        

        /*====================================================
        Clean & Load erp_cust_az12  (Truncate + Insert)
        ====================================================*/
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                ELSE cid
            END,                                      -- Remove 'NAS' perfix if present

            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END,                                      -- Set future birthdates to NULL

            CASE
                WHEN g IN ('M','MALE') THEN 'Male'
                WHEN g IN ('F','FEMALE') THEN 'Female'
                ELSE 'n/a'
            END                                       -- Normalize gender values and handle unknown cases
        FROM
        (
            SELECT
                cid,
                bdate,
                UPPER(
                    TRIM(
                        REPLACE(REPLACE(gen,CHAR(13),''),CHAR(10),'')
                    )
                ) AS g
            FROM bronze.erp_cust_az12
        ) t;

        SET @end_time = SYSDATETIME();
        PRINT 'Load Duration for erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds';
        PRINT '-----------------------------';


        /*====================================================
        Clean & Load erp_loc_a101  (Truncate + Insert)
        ====================================================*/
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )
        SELECT 
        REPLACE(cid, '-', '') AS cid,

        CASE 
	        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) = 'DE' THEN 'Germany'
	        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) IN ('US', 'USA') THEN 'United States'
            WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
        END AS cntry                                  -- Normalize and handle missing or blank country codes 
        FROM bronze.erp_loc_a101;

        SET @end_time = SYSDATETIME();
        PRINT 'Load Duration for erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds';
        PRINT '-----------------------------';



        /*====================================================
        Clean & Load erp_px_cat_g1v2  (Truncate + Insert)
        ====================================================*/
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            TRIM(
                REPLACE(REPLACE(maintenace,CHAR(13),''),CHAR(10),'')
            )
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = SYSDATETIME();
        PRINT 'Load Duration for erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' Seconds';
        PRINT '-----------------------------';


        SET @batch_end = SYSDATETIME();
        PRINT '==============================================';
        PRINT 'Silver Layer Loaded Successfully';
        PRINT '==============================================';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS VARCHAR) + ' Seconds';

        COMMIT TRANSACTION;
    END TRY


    BEGIN CATCH

        ROLLBACK TRANSACTION;

        PRINT '==========================================================';
        PRINT 'Error occurred while loading Silver Layer';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Line   : ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '==========================================================';

        THROW;            -- re-throws the error properly

    END CATCH

END;
GO

-- Execution..
EXEC silver.sp_load_silver_layer
