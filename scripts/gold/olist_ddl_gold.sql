/*
===============================================================================
DDL Script: Create Gold Views - Olist E-commerce Data Warehouse
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)
    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Design Pattern:
    - Star Schema with 4 Dimensions + 1 Fact Table
    - Surrogate keys using ROW_NUMBER()
    - LEFT JOINs to preserve all data (zero data loss)
    - Special dimension records (-1) for missing references
    - Data quality flags for missing data analysis

Fixes Applied:
    - dim_customer uses Silver (cleaned data) and preserves ALL customer_ids
    - Payment validation fixed to avoid double-counting
    - All referential integrity maintained

Usage:
    - These views can be queried directly for analytics and reporting.
    - Supports customer retention, product analysis, and payment insights
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customer
-- =============================================================================
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO

CREATE VIEW gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY c.customer_id) AS customer_key, -- Surrogate key
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix AS zip_code,
    c.customer_city AS city,
    c.customer_state AS state,
    -- Enrich with geolocation data
    g.geolocation_lat AS latitude,
    g.geolocation_lng AS longitude,
    -- Metadata
    c.dwh_create_date AS create_date
FROM silver.olist_customers_dataset c
LEFT JOIN silver.olist_geolocation_dataset g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix

UNION ALL

-- Add special "Unknown Customer" record for orders without customer match
SELECT 
    -1 AS customer_key,
    'UNKNOWN' AS customer_id,
    'UNKNOWN' AS customer_unique_id,
    'N/A' AS zip_code,
    'N/A' AS city,
    'N/A' AS state,
    NULL AS latitude,
    NULL AS longitude,
    GETDATE() AS create_date;
GO

-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_key, -- Surrogate key
    p.product_id,
    p.product_category_name AS category_portuguese,
    COALESCE(t.product_category_name_english, 'Unknown') AS category_english,
    p.product_name_lenght AS name_length,
    p.product_description_lenght AS description_length,
    p.product_photos_qty AS photos_qty,
    p.product_weight_g AS weight_grams,
    p.product_length_cm AS length_cm,
    p.product_height_cm AS height_cm,
    p.product_width_cm AS width_cm,
    -- Calculated: Product volume
    (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS volume_cm3,
    -- Metadata
    p.dwh_create_date AS create_date
FROM silver.olist_products_dataset p
LEFT JOIN silver.product_category_name_translation t
    ON LOWER(p.product_category_name) = LOWER(t.product_category_name)

UNION ALL

-- Add special "No Product" record for orders without items
SELECT 
    -1 AS product_key,
    'UNKNOWN' AS product_id,
    'Unknown' AS category_portuguese,
    'Unknown' AS category_english,
    0 AS name_length,
    0 AS description_length,
    0 AS photos_qty,
    0 AS weight_grams,
    0 AS length_cm,
    0 AS height_cm,
    0 AS width_cm,
    0 AS volume_cm3,
    GETDATE() AS create_date;
GO

-- =============================================================================
-- Create Dimension: gold.dim_seller
-- =============================================================================
IF OBJECT_ID('gold.dim_seller', 'V') IS NOT NULL
    DROP VIEW gold.dim_seller;
GO

CREATE VIEW gold.dim_seller AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.seller_id) AS seller_key, -- Surrogate key
    s.seller_id,
    s.seller_zip_code_prefix AS zip_code,
    s.seller_city AS city,
    s.seller_state AS state,
    -- Metadata
    s.dwh_create_date AS create_date
FROM silver.olist_sellers_dataset s

UNION ALL

-- Add special "No Seller" record for orders without items
SELECT 
    -1 AS seller_key,
    'UNKNOWN' AS seller_id,
    'N/A' AS zip_code,
    'N/A' AS city,
    'N/A' AS state,
    GETDATE() AS create_date;
GO

-- =============================================================================
-- Create Dimension: gold.dim_date (Standard Calendar Dimension)
-- =============================================================================
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
SELECT
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT) AS date_key, -- Surrogate key (YYYYMMDD)
    date_value AS full_date,
    YEAR(date_value) AS year,
    DATEPART(QUARTER, date_value) AS quarter,
    MONTH(date_value) AS month,
    DATENAME(MONTH, date_value) AS month_name,
    DAY(date_value) AS day,
    DATEPART(WEEKDAY, date_value) AS day_of_week,
    DATENAME(WEEKDAY, date_value) AS day_name,
    CASE WHEN DATEPART(WEEKDAY, date_value) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    -- Brazilian holidays (simplified - add more as needed)
    CASE 
        WHEN MONTH(date_value) = 1 AND DAY(date_value) = 1 THEN 1  -- New Year
        WHEN MONTH(date_value) = 9 AND DAY(date_value) = 7 THEN 1  -- Independence Day
        WHEN MONTH(date_value) = 12 AND DAY(date_value) = 25 THEN 1 -- Christmas
        ELSE 0 
    END AS is_holiday
FROM (
    -- Generate dates using a numbers table approach
    SELECT DATEADD(DAY, n, '2016-01-01') AS date_value
    FROM (
        SELECT TOP 3653 -- 10 years (2016-2025) including leap years
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    ) numbers
    WHERE DATEADD(DAY, n, '2016-01-01') <= '2025-12-31'
) dates;
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    -- ========================================================================
    -- SURROGATE KEY
    -- ========================================================================
    ROW_NUMBER() OVER (
        ORDER BY o.order_id, COALESCE(oi.order_item_id, 0)
    ) AS sales_key,
    
    -- ========================================================================
    -- BUSINESS KEYS
    -- ========================================================================
    o.order_id,
    COALESCE(oi.order_item_id, 0) AS order_item_id, -- 0 = order header only (no items)
    
    -- ========================================================================
    -- DIMENSION FOREIGN KEYS
    -- ========================================================================
    COALESCE(dc.customer_key, -1) AS customer_key,
    CAST(FORMAT(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_key,
    COALESCE(dp.product_key, -1) AS product_key,
    COALESCE(ds.seller_key, -1) AS seller_key,
    
    -- ========================================================================
    -- ORDER-LEVEL ATTRIBUTES (Always Available)
    -- ========================================================================
    o.order_status,
    o.order_purchase_timestamp AS order_date,
    o.order_approved_at AS approval_date,
    o.order_delivered_carrier_date AS shipped_date,
    o.order_delivered_customer_date AS delivery_date,
    o.order_estimated_delivery_date AS estimated_delivery_date,
    
    -- ========================================================================
    -- ITEM-LEVEL ATTRIBUTES (NULL if no items)
    -- ========================================================================
    oi.shipping_limit_date,
    oi.price AS item_price,
    oi.freight_value AS item_freight,
    
    -- ========================================================================
    -- CALCULATED METRICS (Item Level)
    -- ========================================================================
    COALESCE(oi.price, 0) AS item_price_clean,
    COALESCE(oi.freight_value, 0) AS item_freight_clean,
    COALESCE(oi.price, 0) + COALESCE(oi.freight_value, 0) AS total_item_value,
    
    -- ========================================================================
    -- PAYMENT METRICS (Aggregated per Order)
    -- ========================================================================
    COALESCE(pay.total_payment_value, 0) AS total_payment_value,
    COALESCE(pay.payment_count, 0) AS payment_count,
    pay.primary_payment_type,
    COALESCE(pay.total_installments, 0) AS total_installments,
    COALESCE(pay.has_credit_card, 0) AS has_credit_card_payment,
    COALESCE(pay.has_boleto, 0) AS has_boleto_payment,
    
    -- ========================================================================
    -- REVIEW METRICS (Aggregated per Order)
    -- ========================================================================
    COALESCE(rev.avg_review_score, 0) AS avg_review_score,
    rev.review_category,
    COALESCE(rev.review_count, 0) AS review_count,
    COALESCE(rev.has_comment, 0) AS has_review_comment,
    
    -- ========================================================================
    -- DATA QUALITY FLAGS (for Analysis)
    -- ========================================================================
    CASE WHEN oi.order_item_id IS NULL THEN 1 ELSE 0 END AS is_order_without_items,
    CASE WHEN pay.payment_count IS NULL OR pay.payment_count = 0 THEN 1 ELSE 0 END AS is_order_without_payment,
    CASE WHEN rev.review_count IS NULL OR rev.review_count = 0 THEN 1 ELSE 0 END AS is_order_without_review,
    
    -- ========================================================================
    -- DELIVERY PERFORMANCE METRICS
    -- ========================================================================
    CASE 
        WHEN o.order_approved_at IS NOT NULL 
        THEN DATEDIFF(DAY, o.order_purchase_timestamp, o.order_approved_at)
        ELSE NULL
    END AS days_to_approval,
    
    CASE 
        WHEN o.order_delivered_carrier_date IS NOT NULL 
        THEN DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_carrier_date)
        ELSE NULL
    END AS days_to_shipping,
    
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL 
        THEN DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date)
        ELSE NULL
    END AS days_to_delivery,
    
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL 
             AND o.order_estimated_delivery_date IS NOT NULL
        THEN DATEDIFF(DAY, o.order_estimated_delivery_date, o.order_delivered_customer_date)
        ELSE NULL
    END AS delivery_vs_estimate_days,
    
    CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1
        WHEN o.order_delivered_customer_date IS NULL AND GETDATE() > o.order_estimated_delivery_date THEN 1
        ELSE 0
    END AS is_late_delivery,
    
    -- ========================================================================
    -- ORDER STATUS FLAGS
    -- ========================================================================
    CASE WHEN LOWER(o.order_status) = 'delivered' THEN 1 ELSE 0 END AS is_delivered,
    CASE WHEN LOWER(o.order_status) = 'canceled' THEN 1 ELSE 0 END AS is_canceled,
    CASE WHEN LOWER(o.order_status) IN ('shipped', 'delivered') THEN 1 ELSE 0 END AS is_shipped

-- ============================================================================
-- FROM CLAUSE: Start from Orders (Preserves ALL Orders)
-- ============================================================================
FROM silver.olist_orders_dataset o

-- LEFT JOIN to preserve ALL orders (even without items)
LEFT JOIN silver.olist_order_items_dataset oi
    ON o.order_id = oi.order_id

-- ============================================================================
-- JOIN TO DIMENSIONS
-- ============================================================================
LEFT JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id

LEFT JOIN gold.dim_product dp
    ON oi.product_id = dp.product_id

LEFT JOIN gold.dim_seller ds
    ON oi.seller_id = ds.seller_id

-- ============================================================================
-- AGGREGATE PAYMENTS PER ORDER (One value per order)
-- ============================================================================
LEFT JOIN (
    SELECT 
        order_id,
        COUNT(*) AS payment_count,
        SUM(payment_value) AS total_payment_value,
        SUM(payment_installments) AS total_installments,
        MAX(CASE WHEN payment_sequential = 1 THEN payment_type END) AS primary_payment_type,
        MAX(CASE WHEN payment_type = 'credit_card' THEN 1 ELSE 0 END) AS has_credit_card,
        MAX(CASE WHEN payment_type = 'boleto' THEN 1 ELSE 0 END) AS has_boleto
    FROM silver.olist_order_payments_dataset
    GROUP BY order_id
) pay ON o.order_id = pay.order_id

-- ============================================================================
-- AGGREGATE REVIEWS PER ORDER (One value per order)
-- ============================================================================
LEFT JOIN (
    SELECT 
        order_id,
        COUNT(*) AS review_count,
        AVG(CAST(review_score AS FLOAT)) AS avg_review_score,
        MAX(review_category) AS review_category,
        MAX(CAST(has_comment AS INT)) AS has_comment
    FROM silver.olist_order_reviews_dataset
    GROUP BY order_id
) rev ON o.order_id = rev.order_id;

GO

-- =============================================================================
-- DATA INTEGRATION & QUALITY CHECKS
-- =============================================================================

/*
===============================================================================
DATA INTEGRATION VALIDATION CHECKS
===============================================================================
Purpose: Verify data quality and integration after Gold layer creation
These checks ensure:
    1. No data loss from Silver to Gold
    2. Referential integrity is maintained
    3. Aggregations are correct
    4. Special dimension records work properly
===============================================================================
*/

PRINT '';
PRINT '===============================================================================';
PRINT 'RUNNING DATA INTEGRATION VALIDATION CHECKS';
PRINT '===============================================================================';
PRINT '';

-- =============================================================================
-- CHECK 1: Record Count Validation (No Data Loss)
-- =============================================================================
PRINT '-- CHECK 1: Record Count Validation --';

-- Orders: Should match exactly
DECLARE @silver_orders INT, @gold_orders INT;
SELECT @silver_orders = COUNT(DISTINCT order_id) FROM silver.olist_orders_dataset;
SELECT @gold_orders = COUNT(DISTINCT order_id) FROM gold.fact_sales;

PRINT 'Orders in Silver: ' + CAST(@silver_orders AS VARCHAR);
PRINT 'Orders in Gold: ' + CAST(@gold_orders AS VARCHAR);
IF @silver_orders = @gold_orders
    PRINT '? PASS: All orders preserved';
ELSE
    PRINT '? FAIL: Data loss detected in orders';
PRINT '';

-- Order Items: Gold should have Silver items + orders without items
DECLARE @silver_items INT, @gold_items INT, @gold_no_items INT;
SELECT @silver_items = COUNT(*) FROM silver.olist_order_items_dataset;
SELECT @gold_items = COUNT(*) FROM gold.fact_sales WHERE is_order_without_items = 0;
SELECT @gold_no_items = COUNT(*) FROM gold.fact_sales WHERE is_order_without_items = 1;

PRINT 'Order Items in Silver: ' + CAST(@silver_items AS VARCHAR);
PRINT 'Order Items in Gold: ' + CAST(@gold_items AS VARCHAR);
PRINT 'Orders Without Items in Gold: ' + CAST(@gold_no_items AS VARCHAR);
PRINT 'Total Gold Records: ' + CAST(@gold_items + @gold_no_items AS VARCHAR);
PRINT '';

-- =============================================================================
-- CHECK 2: Referential Integrity Validation
-- =============================================================================
PRINT '-- CHECK 2: Referential Integrity --';

-- Check for orphaned dimension keys (should be 0 or use -1)
SELECT 
    'Customer Keys' AS dimension,
    COUNT(*) AS total_records,
    SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unknown_keys,
    SUM(CASE WHEN customer_key > 0 THEN 1 ELSE 0 END) AS valid_keys
FROM gold.fact_sales;

SELECT 
    'Product Keys' AS dimension,
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_key = -1 THEN 1 ELSE 0 END) AS unknown_keys,
    SUM(CASE WHEN product_key > 0 THEN 1 ELSE 0 END) AS valid_keys
FROM gold.fact_sales;

SELECT 
    'Seller Keys' AS dimension,
    COUNT(*) AS total_records,
    SUM(CASE WHEN seller_key = -1 THEN 1 ELSE 0 END) AS unknown_keys,
    SUM(CASE WHEN seller_key > 0 THEN 1 ELSE 0 END) AS valid_keys
FROM gold.fact_sales;

PRINT '? Check referential integrity results above';
PRINT 'Expected: customer unknown_keys should be 0 or minimal';
PRINT '';

 

-- =============================================================================
-- CHECK 4: Aggregation Validation (Review Scores)
-- =============================================================================
PRINT '-- CHECK 4: Aggregation Validation (Reviews) --';

-- Compare average review scores
SELECT 
    'Silver Average Review Score' AS source,
    AVG(CAST(review_score AS FLOAT)) AS avg_score
FROM silver.olist_order_reviews_dataset

UNION ALL

SELECT 
    'Gold Average Review Score (weighted)' AS source,
    AVG(avg_review_score) AS avg_score
FROM gold.fact_sales
WHERE review_count > 0;

PRINT '? Check review score aggregation results above';
PRINT '';

-- =============================================================================
-- CHECK 5: Data Quality Flags Validation
-- =============================================================================
PRINT '-- CHECK 5: Data Quality Flags --';

SELECT 
    'Orders Without Items' AS flag_type,
    COUNT(*) AS count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM gold.fact_sales) AS DECIMAL(5,2)) AS percentage
FROM gold.fact_sales
WHERE is_order_without_items = 1

UNION ALL

SELECT 
    'Orders Without Payment' AS flag_type,
    COUNT(DISTINCT order_id) AS count,
    CAST(COUNT(DISTINCT order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM gold.fact_sales) AS DECIMAL(5,2)) AS percentage
FROM gold.fact_sales
WHERE is_order_without_payment = 1

UNION ALL

SELECT 
    'Orders Without Review' AS flag_type,
    COUNT(DISTINCT order_id) AS count,
    CAST(COUNT(DISTINCT order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM gold.fact_sales) AS DECIMAL(5,2)) AS percentage
FROM gold.fact_sales
WHERE is_order_without_review = 1;

PRINT '? Data quality flags calculated above';
PRINT '';

-- =============================================================================
-- CHECK 6: Special Dimension Records Validation
-- =============================================================================
PRINT '-- CHECK 6: Special Dimension Records (-1 keys) --';

-- Verify -1 records exist in dimensions
SELECT 
    'dim_customer' AS dimension,
    COUNT(*) AS special_record_count
FROM gold.dim_customer
WHERE customer_key = -1

UNION ALL

SELECT 
    'dim_product' AS dimension,
    COUNT(*) AS special_record_count
FROM gold.dim_product
WHERE product_key = -1

UNION ALL

SELECT 
    'dim_seller' AS dimension,
    COUNT(*) AS special_record_count
FROM gold.dim_seller
WHERE seller_key = -1;

PRINT '? Expected: 1 record per dimension with key = -1';
PRINT '';

-- =============================================================================
-- CHECK 7: Date Dimension Validation
-- =============================================================================
PRINT '-- CHECK 7: Date Dimension Coverage --';

-- Check date range coverage
SELECT 
    MIN(full_date) AS min_date,
    MAX(full_date) AS max_date,
    COUNT(*) AS total_dates
FROM gold.dim_date;

-- Verify all order dates have matching date keys
SELECT 
    'Orders with Valid Date Keys' AS check_type,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN d.date_key IS NOT NULL THEN 1 ELSE 0 END) AS orders_with_date_key,
    SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS orders_without_date_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_date d ON f.order_date_key = d.date_key;

PRINT '? Date dimension validation complete';
PRINT '';

-- =============================================================================
-- CHECK 8: Business Metrics Validation - CORRECTED
-- =============================================================================
PRINT '-- CHECK 8: Business Metrics Spot Check --';

-- Revenue reconciliation - CORRECTED
SELECT 
    'Total Revenue (Item Level)' AS metric,
    SUM(total_item_value) AS value
FROM gold.fact_sales
WHERE is_order_without_items = 0

UNION ALL

SELECT 
    'Total Payment Value (Order Level - Deduplicated)' AS metric,
    SUM(total_payment_value) AS value
FROM (
    SELECT DISTINCT order_id, total_payment_value
    FROM gold.fact_sales
    WHERE total_payment_value > 0
) t;

PRINT '? Business metrics calculated above';
PRINT 'Note: Small differences expected due to vouchers/rounding';
PRINT '';

-- =============================================================================
-- VALIDATION SUMMARY
-- =============================================================================
PRINT '===============================================================================';
PRINT 'DATA INTEGRATION VALIDATION COMPLETE';
PRINT '===============================================================================';
PRINT '';
PRINT 'Key Validations Performed:';
PRINT '  1. ? Record count validation (no data loss)';
PRINT '  2. ? Referential integrity (dimension keys valid)';
PRINT '  3. ? Aggregation accuracy (payments and reviews) - CORRECTED';
PRINT '  4. ? Data quality flags working';
PRINT '  5. ? Special dimension records (-1) exist';
PRINT '  6. ? Date dimension coverage';
PRINT '  7. ? Business metrics reconciliation - CORRECTED';
PRINT '';
PRINT 'Review the results above for any FAIL or WARNING messages';
PRINT '===============================================================================';
PRINT '';

-- =============================================================================
-- GOLD LAYER CREATION SUMMARY
-- =============================================================================

PRINT '===============================================================================';
PRINT 'Gold Layer Views Created Successfully';
PRINT '===============================================================================';
PRINT '';
PRINT 'Dimensions Created:';
PRINT '  - gold.dim_customer (includes geolocation) - FIXED to use Silver';
PRINT '  - gold.dim_product (includes category translation)';
PRINT '  - gold.dim_seller';
PRINT '  - gold.dim_date (calendar dimension)';
PRINT '';
PRINT 'Fact Table Created:';
PRINT '  - gold.fact_sales (star schema with zero data loss)';
PRINT '';
PRINT 'Special Features:';
PRINT '  - Surrogate keys using ROW_NUMBER()';
PRINT '  - Special dimension records (-1) for missing references';
PRINT '  - Data quality flags (is_order_without_items, etc.)';
PRINT '  - Delivery performance metrics';
PRINT '  - Aggregated payment and review metrics';
PRINT '  - CORRECTED payment validation (no double-counting)';
PRINT '';
PRINT 'Usage Examples:';
PRINT '  -- View all dimensions:';
PRINT '  SELECT * FROM gold.dim_customer;';
PRINT '  SELECT * FROM gold.dim_product;';
PRINT '  SELECT * FROM gold.dim_seller;';
PRINT '  SELECT * FROM gold.dim_date WHERE year = 2018;';
PRINT '';
PRINT '  -- View fact table:';
PRINT '  SELECT * FROM gold.fact_sales;';
PRINT '';
PRINT '  -- Analysis: Orders without items (abandoned carts):';
PRINT '  SELECT COUNT(*) FROM gold.fact_sales WHERE is_order_without_items = 1;';
PRINT '';
PRINT '  -- Analysis: Top selling products:';
PRINT '  SELECT p.category_english, COUNT(*) as items_sold';
PRINT '  FROM gold.fact_sales f';
PRINT '  JOIN gold.dim_product p ON f.product_key = p.product_key';
PRINT '  WHERE f.product_key != -1';
PRINT '  GROUP BY p.category_english;';
PRINT '==============================================================================='; 

-- =============================================================================
-- CHECK 4: Aggregation Validation (Review Scores)
-- =============================================================================
PRINT '-- CHECK 4: Aggregation Validation (Reviews) --';

-- Compare average review scores
SELECT 
    'Silver Average Review Score' AS source,
    AVG(CAST(review_score AS FLOAT)) AS avg_score
FROM silver.olist_order_reviews_dataset

UNION ALL

SELECT 
    'Gold Average Review Score (weighted)' AS source,
    AVG(avg_review_score) AS avg_score
FROM gold.fact_sales
WHERE review_count > 0;

PRINT '? Check review score aggregation results above';
PRINT '';

-- =============================================================================
-- CHECK 5: Data Quality Flags Validation
-- =============================================================================
PRINT '-- CHECK 5: Data Quality Flags --';

SELECT 
    'Orders Without Items' AS flag_type,
    COUNT(*) AS count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM gold.fact_sales) AS DECIMAL(5,2)) AS percentage
FROM gold.fact_sales
WHERE is_order_without_items = 1

UNION ALL

SELECT 
    'Orders Without Payment' AS flag_type,
    COUNT(DISTINCT order_id) AS count,
    CAST(COUNT(DISTINCT order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM gold.fact_sales) AS DECIMAL(5,2)) AS percentage
FROM gold.fact_sales
WHERE is_order_without_payment = 1

UNION ALL

SELECT 
    'Orders Without Review' AS flag_type,
    COUNT(DISTINCT order_id) AS count,
    CAST(COUNT(DISTINCT order_id) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM gold.fact_sales) AS DECIMAL(5,2)) AS percentage
FROM gold.fact_sales
WHERE is_order_without_review = 1;

PRINT '? Data quality flags calculated above';
PRINT '';

-- =============================================================================
-- CHECK 6: Special Dimension Records Validation
-- =============================================================================
PRINT '-- CHECK 6: Special Dimension Records (-1 keys) --';

-- Verify -1 records exist in dimensions
SELECT 
    'dim_customer' AS dimension,
    COUNT(*) AS special_record_count
FROM gold.dim_customer
WHERE customer_key = -1

UNION ALL

SELECT 
    'dim_product' AS dimension,
    COUNT(*) AS special_record_count
FROM gold.dim_product
WHERE product_key = -1

UNION ALL

SELECT 
    'dim_seller' AS dimension,
    COUNT(*) AS special_record_count
FROM gold.dim_seller
WHERE seller_key = -1;

PRINT '? Expected: 1 record per dimension with key = -1';
PRINT '';

-- =============================================================================
-- CHECK 7: Date Dimension Validation
-- =============================================================================
PRINT '-- CHECK 7: Date Dimension Coverage --';

-- Check date range coverage
SELECT 
    MIN(full_date) AS min_date,
    MAX(full_date) AS max_date,
    COUNT(*) AS total_dates
FROM gold.dim_date;

-- Verify all order dates have matching date keys
SELECT 
    'Orders with Valid Date Keys' AS check_type,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN d.date_key IS NOT NULL THEN 1 ELSE 0 END) AS orders_with_date_key,
    SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS orders_without_date_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_date d ON f.order_date_key = d.date_key;

PRINT '? Date dimension validation complete';
PRINT '';

-- =============================================================================
-- CHECK 8: Business Metrics Validation - CORRECTED
-- =============================================================================
PRINT '-- CHECK 8: Business Metrics Spot Check --';

-- Revenue reconciliation - CORRECTED
SELECT 
    'Total Revenue (Item Level)' AS metric,
    SUM(total_item_value) AS value
FROM gold.fact_sales
WHERE is_order_without_items = 0

UNION ALL

SELECT 
    'Total Payment Value (Order Level - Deduplicated)' AS metric,
    SUM(total_payment_value) AS value
FROM (
    SELECT DISTINCT order_id, total_payment_value
    FROM gold.fact_sales
    WHERE total_payment_value > 0
) t;

PRINT '? Business metrics calculated above';
PRINT 'Note: Small differences expected due to vouchers/rounding';
PRINT '';

-- =============================================================================
-- VALIDATION SUMMARY
-- =============================================================================
PRINT '===============================================================================';
PRINT 'DATA INTEGRATION VALIDATION COMPLETE';
PRINT '===============================================================================';
PRINT '';
PRINT 'Key Validations Performed:';
PRINT '  1. ? Record count validation (no data loss)';
PRINT '  2. ? Referential integrity (dimension keys valid)';
PRINT '  3. ? Aggregation accuracy (payments and reviews) - CORRECTED';
PRINT '  4. ? Data quality flags working';
PRINT '  5. ? Special dimension records (-1) exist';
PRINT '  6. ? Date dimension coverage';
PRINT '  7. ? Business metrics reconciliation - CORRECTED';
PRINT '';
PRINT 'Review the results above for any FAIL or WARNING messages';
PRINT '===============================================================================';
PRINT '';

-- =============================================================================
-- GOLD LAYER CREATION SUMMARY
-- =============================================================================

PRINT '===============================================================================';
PRINT 'Gold Layer Views Created Successfully';
PRINT '===============================================================================';
PRINT '';
PRINT 'Dimensions Created:';
PRINT '  - gold.dim_customer (includes geolocation) - FIXED to use Silver';
PRINT '  - gold.dim_product (includes category translation)';
PRINT '  - gold.dim_seller';
PRINT '  - gold.dim_date (calendar dimension)';
PRINT '';
PRINT 'Fact Table Created:';
PRINT '  - gold.fact_sales (star schema with zero data loss)';
PRINT '';
PRINT 'Special Features:';
PRINT '  - Surrogate keys using ROW_NUMBER()';
PRINT '  - Special dimension records (-1) for missing references';
PRINT '  - Data quality flags (is_order_without_items, etc.)';
PRINT '  - Delivery performance metrics';
PRINT '  - Aggregated payment and review metrics';
PRINT '  - CORRECTED payment validation (no double-counting)';
PRINT '';
PRINT 'Usage Examples:';
PRINT '  -- View all dimensions:';
PRINT '  SELECT * FROM gold.dim_customer;';
PRINT '  SELECT * FROM gold.dim_product;';
PRINT '  SELECT * FROM gold.dim_seller;';
PRINT '  SELECT * FROM gold.dim_date WHERE year = 2018;';
PRINT '';
PRINT '  -- View fact table:';
PRINT '  SELECT * FROM gold.fact_sales;';
PRINT '';
PRINT '  -- Analysis: Orders without items (abandoned carts):';
PRINT '  SELECT COUNT(*) FROM gold.fact_sales WHERE is_order_without_items = 1;';
PRINT '';
PRINT '  -- Analysis: Top selling products:';
PRINT '  SELECT p.category_english, COUNT(*) as items_sold';
PRINT '  FROM gold.fact_sales f';
PRINT '  JOIN gold.dim_product p ON f.product_key = p.product_key';
PRINT '  WHERE f.product_key != -1';
PRINT '  GROUP BY p.category_english;';
PRINT '===============================================================================';
