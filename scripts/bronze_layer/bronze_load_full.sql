/*****************************************************************************************
Procedure Name: bronze.load_bronze
Layer         : Bronze Layer (Raw Ingestion Layer)
Load Type     : Full Load (Truncate & Reload)

Description:
This stored procedure performs a full batch load of all source CRM and ERP CSV files 
into the Bronze schema tables.

For each table, the procedure:
1. Truncates the existing data.
2. Bulk loads fresh data from the source CSV files.
3. Logs execution time for monitoring purposes.

The Bronze layer stores raw, untransformed source data exactly as received.
This layer serves as the foundation for downstream transformations into
Silver and Gold layers within the Data Warehouse architecture.

Error Handling:
A TRY/CATCH block captures and prints detailed error information 
to assist in troubleshooting load failures.

Execution:
EXEC bronze.load_bronze;

******************************************************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time      DATETIME2,
        @end_time        DATETIME2,
        @batch_start     DATETIME2,
        @batch_end       DATETIME2;

    BEGIN TRY

        SET @batch_start = SYSDATETIME();

        PRINT '==========================================================';
        PRINT 'Starting Bronze Layer Full Load';
        PRINT '==========================================================';

        ----------------------------------------------------------
        -- CRM TABLES
        ----------------------------------------------------------

        PRINT 'Loading CRM Tables...';

        /* ================= CRM CUSTOMER ================= */

        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\DE_Practical\Data_warehouse_project\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK,
            MAXERRORS = 100
        );

        SET @end_time = SYSDATETIME();
        PRINT 'crm_cust_info loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) 
              + ' seconds.';


        /* ================= CRM PRODUCT ================= */

        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\DE_Practical\Data_warehouse_project\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK,
            MAXERRORS = 100
        );

        SET @end_time = SYSDATETIME();
        PRINT 'crm_prd_info loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) 
              + ' seconds.';


        /* ================= CRM SALES ================= */

        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\DE_Practical\Data_warehouse_project\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK,
            MAXERRORS = 100
        );

        SET @end_time = SYSDATETIME();
        PRINT 'crm_sales_details loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) 
              + ' seconds.';


        ----------------------------------------------------------
        -- ERP TABLES
        ----------------------------------------------------------

        PRINT 'Loading ERP Tables...';

        /* ================= ERP CUSTOMER ================= */

        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\DE_Practical\Data_warehouse_project\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK,
            MAXERRORS = 100
        );

        SET @end_time = SYSDATETIME();
        PRINT 'erp_cust_az12 loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) 
              + ' seconds.';


        /* ================= ERP LOCATION ================= */

        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\DE_Practical\Data_warehouse_project\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK,
            MAXERRORS = 100
        );

        SET @end_time = SYSDATETIME();
        PRINT 'erp_loc_a101 loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) 
              + ' seconds.';


        /* ================= ERP PRODUCT CATEGORY ================= */

        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\DE_Practical\Data_warehouse_project\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            CODEPAGE = '65001',
            TABLOCK,
            MAXERRORS = 100
        );

        SET @end_time = SYSDATETIME();
        PRINT 'erp_px_cat_g1v2 loaded in ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) 
              + ' seconds.';


        ----------------------------------------------------------
        -- BATCH COMPLETION
        ----------------------------------------------------------

        SET @batch_end = SYSDATETIME();

        PRINT '==========================================================';
        PRINT 'Bronze Layer Loaded Successfully';
        PRINT 'Total Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS VARCHAR) 
              + ' seconds.';
        PRINT '==========================================================';

    END TRY
    BEGIN CATCH

        PRINT '==========================================================';
        PRINT 'Error occurred while loading Bronze Layer';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Line   : ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '==========================================================';

        THROW;  -- re-throws the error properly

    END CATCH
END;
GO

EXEC bronze.load_bronze;
