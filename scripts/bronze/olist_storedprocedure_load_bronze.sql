/*
===============================================================================
Stored Procedure: Load Bronze Layer - Olist Dataset (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files
    for the Olist e-commerce dataset. It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.
    - Tracks timing for each table load operation.
    - Implements error handling with detailed logging.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

Tables Loaded:
    1. olist_customers_dataset
    2. olist_geolocation_dataset
    3. olist_order_items_dataset
    4. olist_order_payments_dataset
    5. olist_order_reviews_dataset (with error tolerance)
    6. olist_orders_dataset
    7. olist_products_dataset
    8. olist_sellers_dataset
    9. product_category_name_translation

Notes:
    - olist_order_reviews_dataset has MAXERRORS set to 50000 due to multi-line comments
    - All date/timestamp fields are loaded as VARCHAR and converted in Silver layer
    - File path: C:\dataset\
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @row_count INT;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=========================================================================';
        PRINT 'Loading Bronze Layer - Olist E-commerce Dataset';
        PRINT 'Start Time: ' + CONVERT(VARCHAR, @batch_start_time, 120);
        PRINT '=========================================================================';

        -- =============================================
        -- 1. LOAD CUSTOMERS DATASET
        -- =============================================
        PRINT '--------------------------------------------------------------------------';
        PRINT 'Loading Customer Data';
        PRINT '--------------------------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_customers_dataset';
        TRUNCATE TABLE bronze.olist_customers_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_customers_dataset';
        BULK INSERT bronze.olist_customers_dataset
        FROM 'C:\dataset\olist_customers_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 2. LOAD GEOLOCATION DATASET
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_geolocation_dataset';
        TRUNCATE TABLE bronze.olist_geolocation_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_geolocation_dataset';
        BULK INSERT bronze.olist_geolocation_dataset
        FROM 'C:\dataset\olist_geolocation_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 3. LOAD ORDER ITEMS DATASET
        -- =============================================
        PRINT '--------------------------------------------------------------------------';
        PRINT 'Loading Order Data';
        PRINT '--------------------------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_order_items_dataset';
        TRUNCATE TABLE bronze.olist_order_items_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_order_items_dataset';
        BULK INSERT bronze.olist_order_items_dataset
        FROM 'C:\dataset\olist_order_items_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 4. LOAD ORDER PAYMENTS DATASET
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_order_payments_dataset';
        TRUNCATE TABLE bronze.olist_order_payments_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_order_payments_dataset';
        BULK INSERT bronze.olist_order_payments_dataset
        FROM 'C:\dataset\olist_order_payments_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 5. LOAD ORDER REVIEWS DATASET (WITH ERROR TOLERANCE)
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_order_reviews_dataset';
        TRUNCATE TABLE bronze.olist_order_reviews_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_order_reviews_dataset';
        PRINT '   NOTE: Loading with MAXERRORS=50000 due to multi-line comments in CSV';
        BULK INSERT bronze.olist_order_reviews_dataset
        FROM 'C:\dataset\olist_order_reviews_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            MAXERRORS = 50000,
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 6. LOAD ORDERS DATASET
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_orders_dataset';
        TRUNCATE TABLE bronze.olist_orders_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_orders_dataset';
        BULK INSERT bronze.olist_orders_dataset
        FROM 'C:\dataset\olist_orders_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 7. LOAD PRODUCTS DATASET
        -- =============================================
        PRINT '--------------------------------------------------------------------------';
        PRINT 'Loading Product Data';
        PRINT '--------------------------------------------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_products_dataset';
        TRUNCATE TABLE bronze.olist_products_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_products_dataset';
        BULK INSERT bronze.olist_products_dataset
        FROM 'C:\dataset\olist_products_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 8. LOAD SELLERS DATASET
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.olist_sellers_dataset';
        TRUNCATE TABLE bronze.olist_sellers_dataset;

        PRINT '>> Inserting Data Into: bronze.olist_sellers_dataset';
        BULK INSERT bronze.olist_sellers_dataset
        FROM 'C:\dataset\olist_sellers_dataset.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- 9. LOAD PRODUCT CATEGORY NAME TRANSLATION
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.product_category_name_translation';
        TRUNCATE TABLE bronze.product_category_name_translation;

        PRINT '>> Inserting Data Into: bronze.product_category_name_translation';
        BULK INSERT bronze.product_category_name_translation
        FROM 'C:\dataset\product_category_name_translation.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        
        SET @end_time = GETDATE();
        SET @row_count = @@ROWCOUNT;
        PRINT '   Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
        PRINT '   Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------------------------';

        -- =============================================
        -- SUMMARY
        -- =============================================
        SET @batch_end_time = GETDATE();
        PRINT '=========================================================================';
        PRINT 'Bronze Layer Loading Completed Successfully';
        PRINT '   End Time: ' + CONVERT(VARCHAR, @batch_end_time, 120);
        PRINT '   Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================================================';

        -- Final Row Count Summary
        DECLARE @customers_count INT, @geo_count INT, @items_count INT, @payments_count INT;
        DECLARE @reviews_count INT, @orders_count INT, @products_count INT, @sellers_count INT, @category_count INT;
        
        SELECT @customers_count = COUNT(*) FROM bronze.olist_customers_dataset;
        SELECT @geo_count = COUNT(*) FROM bronze.olist_geolocation_dataset;
        SELECT @items_count = COUNT(*) FROM bronze.olist_order_items_dataset;
        SELECT @payments_count = COUNT(*) FROM bronze.olist_order_payments_dataset;
        SELECT @reviews_count = COUNT(*) FROM bronze.olist_order_reviews_dataset;
        SELECT @orders_count = COUNT(*) FROM bronze.olist_orders_dataset;
        SELECT @products_count = COUNT(*) FROM bronze.olist_products_dataset;
        SELECT @sellers_count = COUNT(*) FROM bronze.olist_sellers_dataset;
        SELECT @category_count = COUNT(*) FROM bronze.product_category_name_translation;
        
        PRINT '';
        PRINT 'Row Count Summary:';
        PRINT '   Customers: ' + CAST(@customers_count AS NVARCHAR);
        PRINT '   Geolocation: ' + CAST(@geo_count AS NVARCHAR);
        PRINT '   Order Items: ' + CAST(@items_count AS NVARCHAR);
        PRINT '   Order Payments: ' + CAST(@payments_count AS NVARCHAR);
        PRINT '   Order Reviews: ' + CAST(@reviews_count AS NVARCHAR);
        PRINT '   Orders: ' + CAST(@orders_count AS NVARCHAR);
        PRINT '   Products: ' + CAST(@products_count AS NVARCHAR);
        PRINT '   Sellers: ' + CAST(@sellers_count AS NVARCHAR);
        PRINT '   Category Translation: ' + CAST(@category_count AS NVARCHAR);
        PRINT '=========================================================================';

    END TRY
    BEGIN CATCH
        PRINT '======================================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT '======================================================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT '======================================================================';
        
        -- Re-throw the error to stop execution
        THROW;
    END CATCH
END
GO

-- Usage Example:
-- EXEC bronze.load_bronze;
