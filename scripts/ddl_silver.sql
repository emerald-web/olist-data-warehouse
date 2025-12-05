/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script defines the cleaned and validated data layer (Silver) for the 
    Olist_DataWarehouse project. It drops existing tables if they exist 
    before recreating them to ensure consistency across runs.

Notes:
    - The Silver layer stores cleaned, validated, and conformed data from Bronze.
    - Data types remain consistent with Bronze layer (transformations happen during load).
    - Metadata columns track data lineage and processing timestamps.
    - These tables serve as the foundation for the Gold layer analytics.

Object Type: Tables
Load Method: Full Load (Truncate and Insert)
Metadata Columns: dwh_create_date (timestamp of record insertion into Silver)

⚠️ Warning:
    Running this script will DROP and RECREATE the Silver tables.
    Any existing data in these tables will be permanently deleted.

Created By: Data Engineering Team
Created Date: 2025-11-13
Last Modified: 2025-11-13
===============================================================================
*/

-- =============================================
-- 1. CUSTOMERS TABLE
-- =============================================
-- Purpose: Stores customer master data with location information
-- Source: bronze.olist_customers_dataset
-- Grain: One row per customer per order (customer_id level)
-- =============================================

IF OBJECT_ID('silver.olist_customers_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_customers_dataset;
GO

CREATE TABLE silver.olist_customers_dataset (
    customer_id              NVARCHAR(50),
    customer_unique_id       NVARCHAR(50),
    customer_zip_code_prefix NVARCHAR(50),
    customer_city            NVARCHAR(150),
    customer_state           NVARCHAR(50),
    
    -- Metadata Columns
    dwh_create_date          DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 2. GEOLOCATION TABLE
-- =============================================
-- Purpose: Geographic reference data linking ZIP codes to coordinates
-- Source: bronze.olist_geolocation_dataset
-- Grain: One row per ZIP code prefix (with duplicates)
-- =============================================

IF OBJECT_ID('silver.olist_geolocation_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_geolocation_dataset;
GO

CREATE TABLE silver.olist_geolocation_dataset (
    geolocation_zip_code_prefix NVARCHAR(10),
    geolocation_lat             DECIMAL(18,14),
    geolocation_lng             DECIMAL(18,14),
    geolocation_city            NVARCHAR(150),
    geolocation_state           NVARCHAR(10),
    
    -- Metadata Columns
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 3. ORDER ITEMS TABLE
-- =============================================
-- Purpose: Line items for each order - products purchased
-- Source: bronze.olist_order_items_dataset
-- Grain: One row per product per order
-- =============================================

IF OBJECT_ID('silver.olist_order_items_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_order_items_dataset;
GO

CREATE TABLE silver.olist_order_items_dataset (
    order_id                NVARCHAR(50),
    order_item_id           INT,
    product_id              NVARCHAR(50),
    seller_id               NVARCHAR(50),
    shipping_limit_date     DATETIME2(0),
    price                   DECIMAL(10,2),
    freight_value           DECIMAL(10,2),
    
    -- Metadata Columns
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 4. ORDER PAYMENTS TABLE
-- =============================================
-- Purpose: Payment information for orders (multiple payments per order possible)
-- Source: bronze.olist_order_payments_dataset
-- Grain: One row per payment per order
-- =============================================

IF OBJECT_ID('silver.olist_order_payments_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_order_payments_dataset;
GO

CREATE TABLE silver.olist_order_payments_dataset (
    order_id                NVARCHAR(50),
    payment_sequential      INT,
    payment_type            NVARCHAR(50),
    payment_installments    INT,
    payment_value           DECIMAL(10,2),
    
    -- Metadata Columns
    dwh_create_date         DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 5. ORDER REVIEWS TABLE
-- =============================================
-- Purpose: Customer reviews and ratings for delivered orders
-- Source: bronze.olist_order_reviews_dataset
-- Grain: One row per review per order
-- Note: Review dates stored as VARCHAR in Bronze, cleaned in Silver load process
-- =============================================

IF OBJECT_ID('silver.olist_order_reviews_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_order_reviews_dataset;
GO

CREATE TABLE silver.olist_order_reviews_dataset (
    review_id                   VARCHAR(50),
    order_id                    VARCHAR(50),
    review_score                INT,
    review_comment_title        VARCHAR(500),
    review_comment_message      VARCHAR(MAX),
    review_creation_date        VARCHAR(100),
    review_answer_timestamp     VARCHAR(100),
    
    -- Metadata Columns
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 6. ORDERS TABLE
-- =============================================
-- Purpose: Core order header information - central fact table
-- Source: bronze.olist_orders_dataset
-- Grain: One row per order
-- =============================================

IF OBJECT_ID('silver.olist_orders_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_orders_dataset;
GO

CREATE TABLE silver.olist_orders_dataset (
    order_id                        NVARCHAR(50),
    customer_id                     NVARCHAR(50),
    order_status                    NVARCHAR(50),
    order_purchase_timestamp        DATETIME2(0),
    order_approved_at               DATETIME2(0),
    order_delivered_carrier_date    DATETIME2(0),
    order_delivered_customer_date   DATETIME2(0),
    order_estimated_delivery_date   DATETIME2(0),
    
    -- Metadata Columns
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 7. PRODUCTS TABLE
-- =============================================
-- Purpose: Product master data with physical characteristics
-- Source: bronze.olist_products_dataset
-- Grain: One row per product
-- =============================================

IF OBJECT_ID('silver.olist_products_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_products_dataset;
GO

CREATE TABLE silver.olist_products_dataset (
    product_id                  NVARCHAR(50),
    product_category_name       NVARCHAR(200),
    product_name_lenght         INT,
    product_description_lenght  INT,
    product_photos_qty          INT,
    product_weight_g            INT,
    product_length_cm           INT,
    product_height_cm           INT,
    product_width_cm            INT,
    
    -- Metadata Columns
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 8. SELLERS TABLE
-- =============================================
-- Purpose: Seller/merchant master data
-- Source: bronze.olist_sellers_dataset
-- Grain: One row per seller
-- =============================================

IF OBJECT_ID('silver.olist_sellers_dataset','U') IS NOT NULL
    DROP TABLE silver.olist_sellers_dataset;
GO

CREATE TABLE silver.olist_sellers_dataset (
    seller_id                   NVARCHAR(50),
    seller_zip_code_prefix      NVARCHAR(10),
    seller_city                 NVARCHAR(150),
    seller_state                NVARCHAR(10),
    
    -- Metadata Columns
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- 9. PRODUCT CATEGORY TRANSLATION TABLE
-- =============================================
-- Purpose: Translation reference for product categories (Portuguese to English)
-- Source: bronze.product_category_name_translation
-- Grain: One row per category
-- =============================================

IF OBJECT_ID('silver.product_category_name_translation','U') IS NOT NULL
    DROP TABLE silver.product_category_name_translation;
GO

CREATE TABLE silver.product_category_name_translation (
    product_category_name           NVARCHAR(200),
    product_category_name_english   NVARCHAR(200),
    
    -- Metadata Columns
    dwh_create_date                 DATETIME2 DEFAULT GETDATE()
);
GO

-- =============================================
-- VERIFICATION QUERY
-- =============================================
-- Query to verify all Silver tables were created successfully
-- =============================================

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'silver'
ORDER BY TABLE_NAME;
GO

/*
===============================================================================
END OF SILVER LAYER DDL SCRIPT
===============================================================================
Expected Output: 9 tables in 'silver' schema

Tables Created:
1. silver.olist_customers_dataset
2. silver.olist_geolocation_dataset
3. silver.olist_order_items_dataset
4. silver.olist_order_payments_dataset
5. silver.olist_order_reviews_dataset
6. silver.olist_orders_dataset
7. silver.olist_products_dataset
8. silver.olist_sellers_dataset
9. silver.product_category_name_translation

Each table includes:
- All original columns from Bronze layer
- Metadata column: dwh_create_date (timestamp of record creation in Silver)

Next Steps:
1. Create Silver layer transformation stored procedures
2. Implement data quality checks
3. Schedule incremental or full refresh loads
===============================================================================
*/
