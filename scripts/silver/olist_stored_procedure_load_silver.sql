/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema for the Olist 
    Brazilian E-commerce dataset.
	
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.
    - Handles data quality issues (quotation marks, NULL values, format standardization).
    - Validates referential integrity between related tables

Parameters:
    None. This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '===============================================================================';
        PRINT 'Loading Silver Layer - Olist Brazilian E-commerce Dataset';
        PRINT '===============================================================================';

        PRINT '-------------------------------------------------------------------------------';
        PRINT 'Loading Customer & Location Tables';
        PRINT '-------------------------------------------------------------------------------';

        -- ====================================================================
        -- 1. Loading silver.olist_customers_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_customers_dataset';
        TRUNCATE TABLE silver.olist_customers_dataset;
        PRINT '>> Inserting Data Into: silver.olist_customers_dataset';
        INSERT INTO silver.olist_customers_dataset (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        SELECT
            -- Remove quotes and trim
            TRIM(REPLACE(customer_id, '"', '')) AS customer_id,
            TRIM(REPLACE(customer_unique_id, '"', '')) AS customer_unique_id,
            TRIM(REPLACE(customer_zip_code_prefix, '"', '')) AS customer_zip_code_prefix,
            
            -- Proper Case for city name: "sao paulo" → "Sao Paulo"
            UPPER(LEFT(TRIM(REPLACE(customer_city, '"', '')), 1)) + 
            LOWER(SUBSTRING(TRIM(REPLACE(customer_city, '"', '')), 2, LEN(TRIM(REPLACE(customer_city, '"', ''))))) 
                AS customer_city,
            
            -- Uppercase state code: "sp" → "SP"
            UPPER(TRIM(REPLACE(customer_state, '"', ''))) AS customer_state
        FROM (
            -- Remove duplicates: Keep one record per customer_unique_id
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(REPLACE(customer_unique_id, '"', ''))
                    ORDER BY customer_id
                ) AS flag_last
            FROM bronze.olist_customers_dataset
            WHERE customer_id IS NOT NULL 
              AND customer_unique_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================================
        -- 2. Loading silver.olist_geolocation_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_geolocation_dataset';
        TRUNCATE TABLE silver.olist_geolocation_dataset;
        PRINT '>> Inserting Data Into: silver.olist_geolocation_dataset';
        INSERT INTO silver.olist_geolocation_dataset (
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        )
        SELECT
            TRIM(REPLACE(geo.geolocation_zip_code_prefix, '"', '')) AS geolocation_zip_code_prefix,
            AVG(geo.geolocation_lat) AS geolocation_lat,
            AVG(geo.geolocation_lng) AS geolocation_lng,
            COALESCE(
                MAX(
                    UPPER(LEFT(TRIM(REPLACE(cust.customer_city, '"', '')), 1)) + 
                    LOWER(SUBSTRING(TRIM(REPLACE(cust.customer_city, '"', '')), 2, 
                          LEN(TRIM(REPLACE(cust.customer_city, '"', '')))))
                ),
                MAX(
                    UPPER(LEFT(TRIM(REPLACE(geo.geolocation_city, '"', '')), 1)) + 
                    LOWER(SUBSTRING(TRIM(REPLACE(geo.geolocation_city, '"', '')), 2, 
                          LEN(TRIM(REPLACE(geo.geolocation_city, '"', '')))))
                )
            ) AS geolocation_city,
            MAX(UPPER(TRIM(REPLACE(geo.geolocation_state, '"', '')))) AS geolocation_state
        FROM bronze.olist_geolocation_dataset geo
        LEFT JOIN bronze.olist_customers_dataset cust
            ON TRIM(REPLACE(geo.geolocation_zip_code_prefix, '"', '')) = 
               TRIM(REPLACE(cust.customer_zip_code_prefix, '"', ''))
        WHERE 
            geo.geolocation_zip_code_prefix IS NOT NULL
            AND TRIM(REPLACE(geo.geolocation_zip_code_prefix, '"', '')) != ''
            AND LEN(TRIM(REPLACE(geo.geolocation_zip_code_prefix, '"', ''))) = 5
            AND TRIM(REPLACE(geo.geolocation_zip_code_prefix, '"', '')) NOT LIKE '%[^0-9]%'
            AND geo.geolocation_lat IS NOT NULL
            AND geo.geolocation_lng IS NOT NULL
            AND geo.geolocation_lat BETWEEN -34 AND 6
            AND geo.geolocation_lng BETWEEN -74 AND -34
        GROUP BY TRIM(REPLACE(geo.geolocation_zip_code_prefix, '"', ''));
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '-------------------------------------------------------------------------------';
        PRINT 'Loading Product & Seller Tables';
        PRINT '-------------------------------------------------------------------------------';

        -- ====================================================================
        -- 3. Loading silver.olist_sellers_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_sellers_dataset';
        TRUNCATE TABLE silver.olist_sellers_dataset;
        PRINT '>> Inserting Data Into: silver.olist_sellers_dataset';
        INSERT INTO silver.olist_sellers_dataset (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )
        SELECT
            TRIM(REPLACE(seller_id, '"', '')) AS seller_id,
            TRIM(REPLACE(seller_zip_code_prefix, '"', '')) AS seller_zip_code_prefix,
            TRIM(REPLACE(seller_city, '"', '')) AS seller_city,
            TRIM(REPLACE(seller_state, '"', '')) AS seller_state
        FROM bronze.olist_sellers_dataset
        WHERE seller_id IS NOT NULL
          AND TRIM(REPLACE(seller_id, '"', '')) != '';
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================================
        -- 4. Loading silver.olist_products_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_products_dataset';
        TRUNCATE TABLE silver.olist_products_dataset;
        PRINT '>> Inserting Data Into: silver.olist_products_dataset';
        INSERT INTO silver.olist_products_dataset (
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            TRIM(REPLACE(product_id, '"', '')) AS product_id,
            CASE 
                WHEN product_category_name IS NULL THEN 'Unknown'
                WHEN LTRIM(RTRIM(product_category_name)) = '' THEN 'Unknown'
                ELSE LTRIM(RTRIM(product_category_name))
            END AS product_category_name,
            product_name_lenght,
            product_description_lenght,
            ISNULL(product_photos_qty, 0) AS product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM bronze.olist_products_dataset
        WHERE product_id IS NOT NULL
          AND TRIM(REPLACE(product_id, '"', '')) != '';
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================================
        -- 5. Loading silver.product_category_name_translation
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.product_category_name_translation';
        TRUNCATE TABLE silver.product_category_name_translation;
        PRINT '>> Inserting Data Into: silver.product_category_name_translation';
        INSERT INTO silver.product_category_name_translation (
            product_category_name,
            product_category_name_english
        )
        SELECT
            TRIM(product_category_name) AS product_category_name,
            TRIM(product_category_name_english) AS product_category_name_english
        FROM bronze.product_category_name_translation
        UNION ALL
        SELECT 'portateis_cozinha_e_preparadores_de_alimentos', 'portable_kitchen_food_processors'
        UNION ALL
        SELECT 'pc_gamer', 'gaming_computers'
        UNION ALL
        SELECT 'Unknown', 'Unknown';
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '-------------------------------------------------------------------------------';
        PRINT 'Loading Order Tables';
        PRINT '-------------------------------------------------------------------------------';

        -- ====================================================================
        -- 6. Loading silver.olist_orders_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_orders_dataset';
        TRUNCATE TABLE silver.olist_orders_dataset;
        PRINT '>> Inserting Data Into: silver.olist_orders_dataset';
        INSERT INTO silver.olist_orders_dataset (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        SELECT
            TRIM(REPLACE(order_id, '"', '')) AS order_id,
            TRIM(REPLACE(customer_id, '"', '')) AS customer_id,
            LOWER(TRIM(REPLACE(order_status, '"', ''))) AS order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        FROM bronze.olist_orders_dataset
        WHERE 
            order_id IS NOT NULL
            AND TRIM(REPLACE(order_id, '"', '')) != ''
            AND customer_id IS NOT NULL
            AND TRIM(REPLACE(customer_id, '"', '')) != ''
            AND order_purchase_timestamp IS NOT NULL
            AND EXISTS (
                SELECT 1 
                FROM bronze.olist_customers_dataset c
                WHERE TRIM(REPLACE(c.customer_id, '"', '')) = TRIM(REPLACE(bronze.olist_orders_dataset.customer_id, '"', ''))
            )
            AND (order_approved_at IS NULL OR order_purchase_timestamp <= order_approved_at)
            AND (order_delivered_carrier_date IS NULL OR order_approved_at IS NULL OR order_approved_at <= order_delivered_carrier_date)
            AND (order_delivered_customer_date IS NULL OR order_delivered_carrier_date IS NULL OR order_delivered_carrier_date <= order_delivered_customer_date);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================================
        -- 7. Loading silver.olist_order_items_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_order_items_dataset';
        TRUNCATE TABLE silver.olist_order_items_dataset;
        PRINT '>> Inserting Data Into: silver.olist_order_items_dataset';
        INSERT INTO silver.olist_order_items_dataset (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        )
        SELECT
            TRIM(REPLACE(order_id, '"', '')) AS order_id,
            order_item_id,
            TRIM(REPLACE(product_id, '"', '')) AS product_id,
            TRIM(REPLACE(seller_id, '"', '')) AS seller_id,
            shipping_limit_date,
            price,
            freight_value
        FROM bronze.olist_order_items_dataset
        WHERE 
            order_id IS NOT NULL
            AND TRIM(REPLACE(order_id, '"', '')) != ''
            AND product_id IS NOT NULL
            AND TRIM(REPLACE(product_id, '"', '')) != ''
            AND seller_id IS NOT NULL
            AND TRIM(REPLACE(seller_id, '"', '')) != ''
            AND EXISTS (
                SELECT 1 
                FROM bronze.olist_orders_dataset o
                WHERE TRIM(REPLACE(o.order_id, '"', '')) = TRIM(REPLACE(bronze.olist_order_items_dataset.order_id, '"', ''))
            )
            AND EXISTS (
                SELECT 1 
                FROM bronze.olist_products_dataset p
                WHERE TRIM(REPLACE(p.product_id, '"', '')) = TRIM(REPLACE(bronze.olist_order_items_dataset.product_id, '"', ''))
            )
            AND EXISTS (
                SELECT 1 
                FROM bronze.olist_sellers_dataset s
                WHERE TRIM(REPLACE(s.seller_id, '"', '')) = TRIM(REPLACE(bronze.olist_order_items_dataset.seller_id, '"', ''))
            );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================================
        -- 8. Loading silver.olist_order_payments_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_order_payments_dataset';
        TRUNCATE TABLE silver.olist_order_payments_dataset;
        PRINT '>> Inserting Data Into: silver.olist_order_payments_dataset';
        INSERT INTO silver.olist_order_payments_dataset (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        SELECT
            TRIM(REPLACE(order_id, '"', '')) AS order_id,
            payment_sequential,
            CASE 
                WHEN LOWER(TRIM(payment_type)) = 'not_defined' THEN 'unknown'
                ELSE LOWER(TRIM(payment_type))
            END AS payment_type,
            payment_installments,
            payment_value
        FROM bronze.olist_order_payments_dataset
        WHERE 
            order_id IS NOT NULL
            AND TRIM(REPLACE(order_id, '"', '')) != ''
            AND EXISTS (
                SELECT 1 
                FROM bronze.olist_orders_dataset o
                WHERE TRIM(REPLACE(o.order_id, '"', '')) = TRIM(REPLACE(bronze.olist_order_payments_dataset.order_id, '"', ''))
            );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- ====================================================================
        -- 9. Loading silver.olist_order_reviews_dataset
        -- ====================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_order_reviews_dataset';
        TRUNCATE TABLE silver.olist_order_reviews_dataset;
        PRINT '>> Inserting Data Into: silver.olist_order_reviews_dataset';
        INSERT INTO silver.olist_order_reviews_dataset (
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp,
            review_category,
            has_comment,
            is_creation_date_valid,
            is_answer_date_valid
        )
        SELECT
            TRIM(REPLACE(review_id, '"', '')) AS review_id,
            TRIM(REPLACE(order_id, '"', '')) AS order_id,
            review_score,
            CASE 
                WHEN review_comment_title IS NULL THEN NULL
                WHEN LTRIM(RTRIM(REPLACE(review_comment_title, '"', ''))) = '' THEN NULL
                ELSE LTRIM(RTRIM(REPLACE(review_comment_title, '"', '')))
            END AS review_comment_title,
            CASE 
                WHEN review_comment_message IS NULL THEN NULL
                WHEN LTRIM(RTRIM(REPLACE(review_comment_message, '"', ''))) = '' THEN NULL
                ELSE LTRIM(RTRIM(REPLACE(review_comment_message, '"', '')))
            END AS review_comment_message,
            CASE 
                WHEN review_creation_date IS NULL THEN NULL
                WHEN LTRIM(RTRIM(review_creation_date)) = '' THEN NULL
                ELSE LTRIM(RTRIM(review_creation_date))
            END AS review_creation_date,
            CASE 
                WHEN review_answer_timestamp IS NULL THEN NULL
                WHEN LTRIM(RTRIM(review_answer_timestamp)) = '' THEN NULL
                ELSE LTRIM(RTRIM(review_answer_timestamp))
            END AS review_answer_timestamp,
            CASE review_score
                WHEN 5 THEN 'Excellent'
                WHEN 4 THEN 'Good'
                WHEN 3 THEN 'Average'
                WHEN 2 THEN 'Poor'
                WHEN 1 THEN 'Very Poor'
                ELSE 'Unknown'
            END AS review_category,
            CASE 
                WHEN (review_comment_title IS NOT NULL AND LTRIM(RTRIM(REPLACE(review_comment_title, '"', ''))) != '')
                     OR (review_comment_message IS NOT NULL AND LTRIM(RTRIM(REPLACE(review_comment_message, '"', ''))) != '')
                THEN 1 
                ELSE 0 
            END AS has_comment,
            CASE 
                WHEN review_creation_date IS NOT NULL 
                     AND LTRIM(RTRIM(review_creation_date)) != ''
                     AND TRY_CONVERT(DATETIME2, review_creation_date) IS NOT NULL
                THEN 1 
                ELSE 0 
            END AS is_creation_date_valid,
            CASE 
                WHEN review_answer_timestamp IS NOT NULL 
                     AND LTRIM(RTRIM(review_answer_timestamp)) != ''
                     AND TRY_CONVERT(DATETIME2, review_answer_timestamp) IS NOT NULL
                THEN 1 
                ELSE 0 
            END AS is_answer_date_valid
        FROM bronze.olist_order_reviews_dataset
        WHERE 
            review_id IS NOT NULL
            AND TRIM(REPLACE(review_id, '"', '')) != ''
            AND order_id IS NOT NULL
            AND TRIM(REPLACE(order_id, '"', '')) != ''
            AND review_score IS NOT NULL
            AND review_score BETWEEN 1 AND 5
            AND EXISTS (
                SELECT 1 
                FROM bronze.olist_orders_dataset o
                WHERE TRIM(REPLACE(o.order_id, '"', '')) = TRIM(REPLACE(bronze.olist_order_reviews_dataset.order_id, '"', ''))
            );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '===============================================================================';
        PRINT 'Loading Silver Layer Completed Successfully';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '===============================================================================';
        
    END TRY
    BEGIN CATCH
        PRINT '===============================================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '===============================================================================';
    END CATCH
END


-- Execute the stored procedure
EXEC silver.load_silver;
