/*
===============================================================================
LOAD SCRIPT: Bronze Layer - Olist CSV Bulk Loads
===============================================================================
Script Purpose:
    Load raw Olist CSV files into the Bronze schema tables. This script truncates
    the target Bronze tables and performs BULK INSERT for each source CSV.

Important Notes / Warnings:
    - This script TRUNCATES the Bronze tables before loading. All existing data
      in those tables will be permanently removed. Run only in dev/test or when
      you explicitly intend to reload Bronze data.
    - BULK INSERT reads files from the SQL Server machine or an accessible UNC
      path. Ensure the SQL Server service account has read access to the folder
      specified by the file paths (e.g., C:\dataset\... or \\server\share\...).
    - If fields are quoted or contain embedded commas/newlines, prefer FORMAT='CSV'
      (SQL Server 2017+) or use a format file. Incorrect FIELDTERMINATOR or ROWTERMINATOR
      settings commonly cause parsing errors.
    - Consider using CODEPAGE = '65001' for UTF-8 files that include non-ASCII characters.
    - Use an ERRORFILE and appropriate MAXERRORS if you want to capture bad rows
      instead of failing the entire load.
    - Review the SELECT/COUNT checks after each load to confirm expected row counts.
    - Keep this script under version control. Record which CSV snapshot was used
      (filename + timestamp) in your run notes or an ops table if needed.

Pre-run Checklist:
    1. Confirm SQL Server service account has read rights to the file paths.
    2. Verify CSV encoding (UTF-8 recommended) and delimiter/quoting conventions.
    3. Confirm you are connected to the correct target database (USE Olist_DataWarehouse).
    4. Backup any valuable Bronze data if you need to preserve it.
    5. Run a small sample BULK INSERT on a copy of the table or a test table to validate parsing.

Example usage:
    -- Open SSMS as an admin, run USE Olist_DataWarehouse; then run this script.
===============================================================================
*/


-- =============================================
-- 1. LOAD CUSTOMERS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_customers_dataset...';
PRINT '========================================';

TRUNCATE TABLE bronze.olist_customers_dataset;

BULK INSERT bronze.olist_customers_dataset
FROM 'C:\dataset\olist_customers_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',  -- Line feed in hex
    TABLOCK
);


SELECT * FROM bronze.olist_customers_dataset


-- =============================================
-- 2. LOAD GEOLOCATION DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_geolocation_dataset...';
PRINT '========================================';

TRUNCATE TABLE bronze.olist_geolocation_dataset;

BULK INSERT bronze.olist_geolocation_dataset
FROM 'C:\dataset\olist_geolocation_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

SELECT * FROM bronze.olist_geolocation_dataset


GO

-- =============================================
-- 3. LOAD ORDER ITEMS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_order_items_dataset...';
PRINT '========================================';

TRUNCATE TABLE bronze.olist_order_items_dataset;

BULK INSERT bronze.olist_order_items_dataset
FROM 'C:\dataset\olist_order_items_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

SELECT * FROM bronze.olist_order_items_dataset
SELECT COUNT(*) FROM bronze.olist_order_items_dataset

GO

-- =============================================
-- 4. LOAD ORDER PAYMENTS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_order_payments_dataset...';
PRINT '========================================';

TRUNCATE TABLE bronze.olist_order_payments_dataset;

BULK INSERT bronze.olist_order_payments_dataset
FROM 'C:\dataset\olist_order_payments_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

SELECT * FROM bronze.olist_order_payments_dataset
SELECT COUNT(*) FROM bronze.olist_order_payments_dataset



GO

-- =============================================
-- 5. LOAD ORDER REVIEWS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_order_reviews_dataset...';
PRINT '========================================';

-- Load with error tolerance
TRUNCATE TABLE bronze.olist_order_reviews_dataset;
-- Load with maximum error tolerance to get most of the data
BULK INSERT bronze.olist_order_reviews_dataset
FROM 'C:\dataset\olist_order_reviews_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    MAXERRORS = 50000,  -- Allow more errors
    ERRORFILE = 'C:\dataset\review_errors.txt',
    TABLOCK
);

-- Check how many loaded
SELECT COUNT(*) FROM bronze.olist_order_reviews_dataset;
-- Expected: ~104, 719 rows
-- If you got 95,000+, that might be acceptable for bronze layer
SELECT * FROM bronze.olist_order_reviews_dataset



GO

-- =============================================
-- 6. LOAD ORDERS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_orders_dataset...';
PRINT '========================================';

BULK INSERT bronze.olist_orders_dataset
FROM 'C:\dataset\olist_orders_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);


SELECT * FROM bronze.olist_orders_dataset
SELECT COUNT(*) FROM bronze.olist_orders_dataset



GO

-- =============================================
-- 7. LOAD PRODUCTS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_products_dataset...';
PRINT '========================================';

BULK INSERT bronze.olist_products_dataset
FROM 'C:\dataset\olist_products_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);



SELECT * FROM bronze.olist_products_dataset
SELECT COUNT(*) FROM bronze.olist_products_dataset


GO

-- =============================================
-- 8. LOAD SELLERS DATASET
-- =============================================
PRINT '========================================';
PRINT 'Loading olist_sellers_dataset...';
PRINT '========================================';

BULK INSERT bronze.olist_sellers_dataset
FROM 'C:\dataset\olist_sellers_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

SELECT * FROM bronze.olist_sellers_dataset
SELECT COUNT(*) FROM bronze.olist_sellers_dataset


GO

-- =============================================
-- 9. LOAD PRODUCT CATEGORY NAME TRANSLATION
-- =============================================
PRINT '========================================';
PRINT 'Loading product_category_name_translation...';
PRINT '========================================';

BULK INSERT bronze.product_category_name_translation
FROM 'C:\dataset\product_category_name_translation.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);




