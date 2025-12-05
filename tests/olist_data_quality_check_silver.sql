/*
===============================================================================
Olist Data Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_customers_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_customers_dataset
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (customer_id)
-- ============================================================================
-- Expectation: No results (all customer_id should be unique and not null)

SELECT 
    customer_id,
    COUNT(*) AS duplicate_count
FROM bronze.olist_customers_dataset
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;

-- ============================================================================
-- 2. CHECK FOR DUPLICATES IN UNIQUE CUSTOMER ID (customer_unique_id)
-- ============================================================================
-- Expectation: customer_unique_id can have duplicates (same customer, multiple orders)
-- But let's see the distribution

SELECT 
    customer_unique_id,
    COUNT(*) AS order_count
FROM bronze.olist_customers_dataset
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY order_count DESC;

PRINT 'Note: Multiple customer_id per customer_unique_id is EXPECTED (repeat customers)';

-- ============================================================================
-- 3. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================
-- Expectation: No results (all fields should be trimmed)

-- Check customer_id
SELECT customer_id
FROM bronze.olist_customers_dataset
WHERE customer_id != TRIM(customer_id);

-- Check customer_unique_id
SELECT customer_unique_id
FROM bronze.olist_customers_dataset
WHERE customer_unique_id != TRIM(customer_unique_id);

-- Check customer_city
SELECT customer_city
FROM bronze.olist_customers_dataset
WHERE customer_city != TRIM(customer_city);

-- Check customer_state
SELECT customer_state
FROM bronze.olist_customers_dataset
WHERE customer_state != TRIM(customer_state);

-- Check customer_zip_code_prefix
SELECT customer_zip_code_prefix
FROM bronze.olist_customers_dataset
WHERE customer_zip_code_prefix != TRIM(customer_zip_code_prefix);

-- ============================================================================
-- 4. DATA STANDARDIZATION & CONSISTENCY - STATE CODES
-- ============================================================================
-- Check for invalid or inconsistent state codes
-- Expectation: Only valid Brazilian state codes (27 states)

SELECT DISTINCT customer_state
FROM bronze.olist_customers_dataset
ORDER BY customer_state;

-- Identify invalid state codes
SELECT DISTINCT customer_state
FROM bronze.olist_customers_dataset
WHERE customer_state NOT IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
    'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
    'RS','RO','RR','SC','SP','SE','TO'
)
ORDER BY customer_state;

-- ============================================================================
-- 5. DATA STANDARDIZATION & CONSISTENCY - CITY NAMES
-- ============================================================================
-- Check city name format (should be lowercase in Bronze)

SELECT DISTINCT 
    customer_city,
    LEN(customer_city) AS city_length
FROM bronze.olist_customers_dataset
WHERE customer_city LIKE '%[A-Z]%' -- Check for uppercase
ORDER BY customer_city;

PRINT 'Note: City names are expected to be lowercase in Bronze layer';

-- ============================================================================
-- 6. ZIP CODE FORMAT VALIDATION
-- ============================================================================
-- Check for invalid ZIP code formats
-- Expectation: All ZIP codes should be 5 digits

SELECT 
    customer_zip_code_prefix,
    LEN(customer_zip_code_prefix) AS zip_length
FROM bronze.olist_customers_dataset
WHERE LEN(customer_zip_code_prefix) != 5
   OR customer_zip_code_prefix LIKE '%[^0-9]%'; -- Contains non-numeric

-- ============================================================================
-- 7. CHECK FOR NULL VALUES IN MANDATORY FIELDS
-- ============================================================================
-- Expectation: No NULL values (all fields are NOT NULL in Bronze DDL)

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) AS null_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS null_state
FROM bronze.olist_customers_dataset;

-- ============================================================================
-- 8. CHECK FOR EMPTY STRING VALUES
-- ============================================================================
-- Expectation: No empty strings

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN customer_id = '' THEN 1 ELSE 0 END) AS empty_customer_id,
    SUM(CASE WHEN customer_unique_id = '' THEN 1 ELSE 0 END) AS empty_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix = '' THEN 1 ELSE 0 END) AS empty_zip,
    SUM(CASE WHEN customer_city = '' THEN 1 ELSE 0 END) AS empty_city,
    SUM(CASE WHEN customer_state = '' THEN 1 ELSE 0 END) AS empty_state
FROM bronze.olist_customers_dataset;

-- ============================================================================
-- 9. DATA INTEGRITY CHECK - CUSTOMER DISTRIBUTION
-- ============================================================================
-- Check customer distribution by state

SELECT 
    customer_state,
    COUNT(DISTINCT customer_unique_id) AS unique_customers,
    COUNT(*) AS total_orders,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_customers_dataset
GROUP BY customer_state
ORDER BY total_orders DESC;

-- ============================================================================
-- 10. CHECK FOR RELATIONSHIP TO GEOLOCATION TABLE
-- ============================================================================
-- Verify that customer ZIP codes exist in geolocation table
-- Expectation: Most should match, some may not

SELECT 
    COUNT(DISTINCT c.customer_zip_code_prefix) AS total_customer_zips,
    COUNT(DISTINCT g.geolocation_zip_code_prefix) AS matching_geo_zips,
    COUNT(DISTINCT c.customer_zip_code_prefix) - 
        COUNT(DISTINCT g.geolocation_zip_code_prefix) AS unmatched_zips
FROM bronze.olist_customers_dataset c
LEFT JOIN bronze.olist_geolocation_dataset g 
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;

-- List ZIP codes with no geolocation data
SELECT DISTINCT 
    c.customer_zip_code_prefix,
    COUNT(*) AS customer_count
FROM bronze.olist_customers_dataset c
LEFT JOIN bronze.olist_geolocation_dataset g 
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL
GROUP BY c.customer_zip_code_prefix
ORDER BY customer_count DESC;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customer_ids,
    COUNT(DISTINCT customer_unique_id) AS unique_customers,
    COUNT(DISTINCT customer_state) AS unique_states,
    COUNT(DISTINCT customer_city) AS unique_cities,
    COUNT(DISTINCT customer_zip_code_prefix) AS unique_zip_codes
FROM bronze.olist_customers_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete';
PRINT 'Review results above to identify data quality issues';
PRINT '===============================================================================';


/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_order_reviews_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_order_reviews_dataset
Key Issues Expected: Quotation marks, NULL values, invalid scores, date format issues
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- ============================================================================
-- Primary Key: review_id
-- Expectation: No duplicates, no NULLs

SELECT 
    review_id,
    COUNT(*) AS duplicate_count
FROM bronze.olist_order_reviews_dataset
GROUP BY review_id
HAVING COUNT(*) > 1 OR review_id IS NULL;

PRINT 'Check 1: Primary key validation - review_id';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT review_id
FROM bronze.olist_order_reviews_dataset
WHERE review_id != TRIM(review_id);

SELECT order_id
FROM bronze.olist_order_reviews_dataset
WHERE order_id != TRIM(order_id);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN review_id LIKE '%"%' THEN 1 ELSE 0 END) AS review_id_with_quotes,
    SUM(CASE WHEN order_id LIKE '%"%' THEN 1 ELSE 0 END) AS order_id_with_quotes,
    SUM(CASE WHEN review_comment_title LIKE '%"%' THEN 1 ELSE 0 END) AS title_with_quotes,
    SUM(CASE WHEN review_comment_message LIKE '%"%' THEN 1 ELSE 0 END) AS message_with_quotes
FROM bronze.olist_order_reviews_dataset;

-- ============================================================================
-- 4. CHECK FOR NULL VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) AS null_review_id,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) AS null_review_score,
    SUM(CASE WHEN review_creation_date IS NULL THEN 1 ELSE 0 END) AS null_creation_date,
    SUM(CASE WHEN review_answer_timestamp IS NULL THEN 1 ELSE 0 END) AS null_answer_timestamp,
    SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END) AS null_comment_title,
    SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) AS null_comment_message
FROM bronze.olist_order_reviews_dataset;

-- ============================================================================
-- 5. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN review_id = '' THEN 1 ELSE 0 END) AS empty_review_id,
    SUM(CASE WHEN order_id = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN review_comment_title = '' THEN 1 ELSE 0 END) AS empty_title,
    SUM(CASE WHEN review_comment_message = '' THEN 1 ELSE 0 END) AS empty_message,
    SUM(CASE WHEN review_creation_date = '' THEN 1 ELSE 0 END) AS empty_creation_date,
    SUM(CASE WHEN review_answer_timestamp = '' THEN 1 ELSE 0 END) AS empty_answer_timestamp
FROM bronze.olist_order_reviews_dataset;

-- ============================================================================
-- 6. VALIDATE REVIEW SCORES
-- ============================================================================
-- Check for scores outside 1-5 range

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN review_score < 1 THEN 1 ELSE 0 END) AS score_below_1,
    SUM(CASE WHEN review_score > 5 THEN 1 ELSE 0 END) AS score_above_5,
    SUM(CASE WHEN review_score NOT BETWEEN 1 AND 5 THEN 1 ELSE 0 END) AS invalid_scores,
    MIN(review_score) AS min_score,
    MAX(review_score) AS max_score,
    AVG(CAST(review_score AS FLOAT)) AS avg_score
FROM bronze.olist_order_reviews_dataset
WHERE review_score IS NOT NULL;

-- Show records with invalid scores
SELECT TOP 10
    review_id,
    order_id,
    review_score
FROM bronze.olist_order_reviews_dataset
WHERE review_score NOT BETWEEN 1 AND 5;

-- ============================================================================
-- 7. REVIEW SCORE DISTRIBUTION
-- ============================================================================

SELECT 
    review_score,
    COUNT(*) AS count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_order_reviews_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_order_reviews_dataset
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;

PRINT 'Expected: Review scores between 1 and 5';

-- ============================================================================
-- 8. CHECK DATE FORMAT ISSUES
-- ============================================================================
-- Since dates are stored as VARCHAR, check for invalid formats

SELECT TOP 10
    review_id,
    order_id,
    review_creation_date,
    review_answer_timestamp,
    LEN(review_creation_date) AS creation_date_length,
    LEN(review_answer_timestamp) AS answer_timestamp_length
FROM bronze.olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL
ORDER BY LEN(review_creation_date) DESC;

-- Check if dates can be converted
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE 
        WHEN review_creation_date IS NOT NULL 
        AND review_creation_date != ''
        AND TRY_CONVERT(DATETIME2, review_creation_date) IS NULL 
        THEN 1 ELSE 0 
    END) AS invalid_creation_dates,
    SUM(CASE 
        WHEN review_answer_timestamp IS NOT NULL 
        AND review_answer_timestamp != ''
        AND TRY_CONVERT(DATETIME2, review_answer_timestamp) IS NULL 
        THEN 1 ELSE 0 
    END) AS invalid_answer_timestamps
FROM bronze.olist_order_reviews_dataset;

-- ============================================================================
-- 9. TEXT CONTENT ANALYSIS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN review_comment_title IS NOT NULL AND LTRIM(RTRIM(review_comment_title)) != '' THEN 1 ELSE 0 END) AS has_title,
    SUM(CASE WHEN review_comment_message IS NOT NULL AND LTRIM(RTRIM(review_comment_message)) != '' THEN 1 ELSE 0 END) AS has_message,
    SUM(CASE WHEN (review_comment_title IS NOT NULL AND LTRIM(RTRIM(review_comment_title)) != '') 
                   OR (review_comment_message IS NOT NULL AND LTRIM(RTRIM(review_comment_message)) != '') THEN 1 ELSE 0 END) AS has_any_text,
    CAST(SUM(CASE WHEN review_comment_message IS NOT NULL AND LTRIM(RTRIM(review_comment_message)) != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_with_message
FROM bronze.olist_order_reviews_dataset;

-- Reviews by score with text analysis
SELECT 
    review_score,
    COUNT(*) AS total_reviews,
    SUM(CASE WHEN review_comment_message IS NOT NULL AND LTRIM(RTRIM(review_comment_message)) != '' THEN 1 ELSE 0 END) AS reviews_with_message,
    CAST(SUM(CASE WHEN review_comment_message IS NOT NULL AND LTRIM(RTRIM(review_comment_message)) != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_with_message
FROM bronze.olist_order_reviews_dataset
GROUP BY review_score
ORDER BY review_score;

-- ============================================================================
-- 10. CHECK FOR ORDERS WITH MULTIPLE REVIEWS
-- ============================================================================

SELECT 
    order_id,
    COUNT(*) AS review_count,
    MIN(review_score) AS min_score,
    MAX(review_score) AS max_score,
    AVG(CAST(review_score AS FLOAT)) AS avg_score
FROM bronze.olist_order_reviews_dataset
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY review_count DESC;

PRINT 'Expected: Most orders should have only 1 review';

-- ============================================================================
-- 11. CHECK RELATIONSHIP TO ORDERS TABLE
-- ============================================================================
-- Verify all order_id exist in orders table

SELECT 
    COUNT(DISTINCT r.order_id) AS total_order_ids,
    COUNT(DISTINCT o.order_id) AS matching_orders,
    COUNT(DISTINCT r.order_id) - COUNT(DISTINCT o.order_id) AS unmatched_orders
FROM bronze.olist_order_reviews_dataset r
LEFT JOIN bronze.olist_orders_dataset o 
    ON TRIM(REPLACE(r.order_id, '"', '')) = TRIM(REPLACE(o.order_id, '"', ''));

-- Show sample of unmatched orders (if any)
SELECT DISTINCT TOP 10
    TRIM(REPLACE(r.order_id, '"', '')) AS review_order_id
FROM bronze.olist_order_reviews_dataset r
LEFT JOIN bronze.olist_orders_dataset o 
    ON TRIM(REPLACE(r.order_id, '"', '')) = TRIM(REPLACE(o.order_id, '"', ''))
WHERE o.order_id IS NULL;

-- ============================================================================
-- 12. DATE LOGIC VALIDATION
-- ============================================================================
-- Check if answer timestamp is after creation date

SELECT 
    COUNT(*) AS total_with_both_dates,
    SUM(CASE 
        WHEN TRY_CONVERT(DATETIME2, review_answer_timestamp) < TRY_CONVERT(DATETIME2, review_creation_date) 
        THEN 1 ELSE 0 
    END) AS answer_before_creation
FROM bronze.olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL 
  AND review_creation_date != ''
  AND review_answer_timestamp IS NOT NULL 
  AND review_answer_timestamp != '';

-- ============================================================================
-- 13. TEMPORAL DISTRIBUTION
-- ============================================================================
-- Review distribution over time

SELECT 
    YEAR(TRY_CONVERT(DATETIME2, review_creation_date)) AS review_year,
    MONTH(TRY_CONVERT(DATETIME2, review_creation_date)) AS review_month,
    COUNT(*) AS review_count,
    AVG(CAST(review_score AS FLOAT)) AS avg_score
FROM bronze.olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL 
  AND review_creation_date != ''
  AND TRY_CONVERT(DATETIME2, review_creation_date) IS NOT NULL
GROUP BY YEAR(TRY_CONVERT(DATETIME2, review_creation_date)), 
         MONTH(TRY_CONVERT(DATETIME2, review_creation_date))
ORDER BY review_year, review_month;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_reviews,
    COUNT(DISTINCT review_id) AS unique_review_ids,
    COUNT(DISTINCT order_id) AS unique_orders,
    AVG(CAST(review_score AS FLOAT)) AS avg_review_score,
    MIN(review_score) AS min_score,
    MAX(review_score) AS max_score
FROM bronze.olist_order_reviews_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Order Reviews';
PRINT 'Key Issues: Quotation marks, date format validation, review score validation';
PRINT '===============================================================================';




-- See the duplicate review_id records in detail
SELECT 
    review_id,
    order_id,
    review_score,
    review_creation_date,
    review_comment_title,
    review_comment_message
FROM bronze.olist_order_reviews_dataset
WHERE review_id IN (
    '"ae57754056fd9da5389d1b1a43ab0983"',
    '"1961cafe1d1cbd4cc701c2f0160467ff"',
    '"5fd4f9ee064426dbf060c2d18b0afe59"'
)
ORDER BY review_id, review_creation_date;

/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_order_payments_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_order_payments_dataset
Key Issues Expected: Quotation marks, payment type validation, installment logic
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN COMPOSITE PRIMARY KEY
-- ============================================================================
-- Primary Key: order_id + payment_sequential
-- Expectation: No duplicates

SELECT 
    order_id,
    payment_sequential,
    COUNT(*) AS duplicate_count
FROM bronze.olist_order_payments_dataset
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1 OR order_id IS NULL OR payment_sequential IS NULL;

PRINT 'Check 1: Composite primary key validation';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT order_id
FROM bronze.olist_order_payments_dataset
WHERE order_id != TRIM(order_id);

SELECT payment_type
FROM bronze.olist_order_payments_dataset
WHERE payment_type != TRIM(payment_type);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id LIKE '%"%' THEN 1 ELSE 0 END) AS order_id_with_quotes,
    SUM(CASE WHEN payment_type LIKE '%"%' THEN 1 ELSE 0 END) AS payment_type_with_quotes
FROM bronze.olist_order_payments_dataset;

-- ============================================================================
-- 4. DATA STANDARDIZATION - PAYMENT TYPES
-- ============================================================================
-- Check all payment type values

SELECT DISTINCT 
    payment_type,
    COUNT(*) AS count
FROM bronze.olist_order_payments_dataset
GROUP BY payment_type
ORDER BY count DESC;

PRINT 'Expected payment types: credit_card, boleto, voucher, debit_card';

-- ============================================================================
-- 5. CHECK FOR NULL VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN payment_sequential IS NULL THEN 1 ELSE 0 END) AS null_payment_seq,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS null_payment_type,
    SUM(CASE WHEN payment_installments IS NULL THEN 1 ELSE 0 END) AS null_installments,
    SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM bronze.olist_order_payments_dataset;

-- ============================================================================
-- 6. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN order_id = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN payment_type = '' THEN 1 ELSE 0 END) AS empty_payment_type
FROM bronze.olist_order_payments_dataset;

-- ============================================================================
-- 7. VALIDATE PAYMENT VALUES
-- ============================================================================
-- Check for negative or zero payment values

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN payment_value < 0 THEN 1 ELSE 0 END) AS negative_payment,
    SUM(CASE WHEN payment_value = 0 THEN 1 ELSE 0 END) AS zero_payment,
    MIN(payment_value) AS min_payment,
    MAX(payment_value) AS max_payment,
    AVG(payment_value) AS avg_payment,
    SUM(payment_value) AS total_payment_value
FROM bronze.olist_order_payments_dataset;

-- Show records with zero or negative payments
SELECT TOP 10
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM bronze.olist_order_payments_dataset
WHERE payment_value <= 0
ORDER BY payment_value;

-- ============================================================================
-- 8. VALIDATE INSTALLMENT LOGIC
-- ============================================================================
-- Check if installments make sense for payment types

-- Show payment types with installments > 1
SELECT 
    payment_type,
    MIN(payment_installments) AS min_installments,
    MAX(payment_installments) AS max_installments,
    AVG(payment_installments) AS avg_installments,
    COUNT(*) AS count
FROM bronze.olist_order_payments_dataset
GROUP BY payment_type
ORDER BY avg_installments DESC;

-- Check for non-credit_card payments with high installments
SELECT 
    payment_type,
    payment_installments,
    COUNT(*) AS count
FROM bronze.olist_order_payments_dataset
WHERE payment_type != 'credit_card'
  AND payment_installments > 1
GROUP BY payment_type, payment_installments
ORDER BY payment_type, payment_installments;

PRINT 'Expected: Only credit_card should have installments > 1';

-- ============================================================================
-- 9. CHECK PAYMENT_SEQUENTIAL LOGIC
-- ============================================================================
-- Check if payment_sequential starts at 1 per order

SELECT 
    order_id,
    COUNT(*) AS payment_count,
    MIN(payment_sequential) AS min_seq,
    MAX(payment_sequential) AS max_seq
FROM bronze.olist_order_payments_dataset
GROUP BY order_id
HAVING MIN(payment_sequential) != 1 OR MAX(payment_sequential) != COUNT(*)
ORDER BY payment_count DESC;

PRINT 'Expected: payment_sequential starts at 1 and increments by 1';

-- ============================================================================
-- 10. CHECK RELATIONSHIP TO ORDERS TABLE
-- ============================================================================
-- Verify all order_id exist in orders table

SELECT 
    COUNT(DISTINCT op.order_id) AS total_order_ids,
    COUNT(DISTINCT o.order_id) AS matching_orders,
    COUNT(DISTINCT op.order_id) - COUNT(DISTINCT o.order_id) AS unmatched_orders
FROM bronze.olist_order_payments_dataset op
LEFT JOIN bronze.olist_orders_dataset o 
    ON TRIM(REPLACE(op.order_id, '"', '')) = TRIM(REPLACE(o.order_id, '"', ''));

-- Show sample of unmatched orders (if any)
SELECT DISTINCT TOP 10
    TRIM(REPLACE(op.order_id, '"', '')) AS payment_order_id
FROM bronze.olist_order_payments_dataset op
LEFT JOIN bronze.olist_orders_dataset o 
    ON TRIM(REPLACE(op.order_id, '"', '')) = TRIM(REPLACE(o.order_id, '"', ''))
WHERE o.order_id IS NULL;

-- ============================================================================
-- 11. MULTIPLE PAYMENTS PER ORDER ANALYSIS
-- ============================================================================
-- How many orders have multiple payments?

SELECT 
    payments_per_order,
    COUNT(*) AS order_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM bronze.olist_order_payments_dataset) AS DECIMAL(5,2)) AS percentage
FROM (
    SELECT order_id, COUNT(*) AS payments_per_order
    FROM bronze.olist_order_payments_dataset
    GROUP BY order_id
) t
GROUP BY payments_per_order
ORDER BY payments_per_order;

-- ============================================================================
-- 12. PAYMENT TYPE DISTRIBUTION
-- ============================================================================

SELECT 
    payment_type,
    COUNT(*) AS payment_count,
    SUM(payment_value) AS total_value,
    AVG(payment_value) AS avg_value,
    AVG(payment_installments) AS avg_installments,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_order_payments_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_order_payments_dataset
GROUP BY payment_type
ORDER BY payment_count DESC;

-- ============================================================================
-- 13. INSTALLMENT DISTRIBUTION (for credit cards)
-- ============================================================================

SELECT 
    payment_installments,
    COUNT(*) AS count,
    AVG(payment_value) AS avg_value
FROM bronze.olist_order_payments_dataset
WHERE payment_type = 'credit_card'
GROUP BY payment_installments
ORDER BY payment_installments;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_payments,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(DISTINCT payment_type) AS unique_payment_types,
    AVG(payment_value) AS avg_payment_value,
    SUM(payment_value) AS total_payment_value,
    AVG(payment_installments) AS avg_installments
FROM bronze.olist_order_payments_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Order Payments';
PRINT 'Key Issues: Quotation marks, validate payment types, check installment logic';
PRINT '===============================================================================';


/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_order_items_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_order_items_dataset
Key Issues Expected: Quotation marks, VARCHAR dates, price/freight validation
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN COMPOSITE PRIMARY KEY
-- ============================================================================
-- Primary Key: order_id + order_item_id
-- Expectation: No duplicates

SELECT 
    order_id,
    order_item_id,
    COUNT(*) AS duplicate_count
FROM bronze.olist_order_items_dataset
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1 OR order_id IS NULL OR order_item_id IS NULL;

PRINT 'Check 1: Composite primary key validation';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT order_id
FROM bronze.olist_order_items_dataset
WHERE order_id != TRIM(order_id);

SELECT product_id
FROM bronze.olist_order_items_dataset
WHERE product_id != TRIM(product_id);

SELECT seller_id
FROM bronze.olist_order_items_dataset
WHERE seller_id != TRIM(seller_id);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id LIKE '%"%' THEN 1 ELSE 0 END) AS order_id_with_quotes,
    SUM(CASE WHEN product_id LIKE '%"%' THEN 1 ELSE 0 END) AS product_id_with_quotes,
    SUM(CASE WHEN seller_id LIKE '%"%' THEN 1 ELSE 0 END) AS seller_id_with_quotes
FROM bronze.olist_order_items_dataset;

-- ============================================================================
-- 4. CHECK SHIPPING_LIMIT_DATE FORMAT
-- ============================================================================
-- Check if stored as VARCHAR or DATETIME

SELECT 
    shipping_limit_date,
    LEN(shipping_limit_date) AS date_length,
    COUNT(*) AS count
FROM bronze.olist_order_items_dataset
WHERE shipping_limit_date IS NOT NULL
GROUP BY shipping_limit_date, LEN(shipping_limit_date)
ORDER BY count DESC;

-- Check data type
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'bronze'
  AND TABLE_NAME = 'olist_order_items_dataset'
  AND COLUMN_NAME = 'shipping_limit_date';

-- ============================================================================
-- 5. CHECK FOR NULL VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) AS null_item_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) AS null_shipping_date,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) AS null_freight
FROM bronze.olist_order_items_dataset;

-- ============================================================================
-- 6. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN order_id = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN product_id = '' THEN 1 ELSE 0 END) AS empty_product_id,
    SUM(CASE WHEN seller_id = '' THEN 1 ELSE 0 END) AS empty_seller_id
FROM bronze.olist_order_items_dataset;

-- ============================================================================
-- 7. VALIDATE PRICE AND FREIGHT VALUES
-- ============================================================================
-- Check for negative or zero values

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS negative_price,
    SUM(CASE WHEN price = 0 THEN 1 ELSE 0 END) AS zero_price,
    SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END) AS negative_freight,
    SUM(CASE WHEN freight_value = 0 THEN 1 ELSE 0 END) AS zero_freight,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    MIN(freight_value) AS min_freight,
    MAX(freight_value) AS max_freight,
    AVG(freight_value) AS avg_freight
FROM bronze.olist_order_items_dataset;

-- Show records with suspicious pricing
SELECT TOP 10
    order_id,
    order_item_id,
    product_id,
    price,
    freight_value,
    price + freight_value AS total_item_cost
FROM bronze.olist_order_items_dataset
WHERE price = 0 OR freight_value < 0 OR price < 0
ORDER BY price, freight_value;

-- ============================================================================
-- 8. CHECK ORDER_ITEM_ID SEQUENCE
-- ============================================================================
-- Check if order_item_id starts at 1 and increments properly per order

SELECT 
    order_id,
    COUNT(*) AS item_count,
    MIN(order_item_id) AS min_item_id,
    MAX(order_item_id) AS max_item_id
FROM bronze.olist_order_items_dataset
GROUP BY order_id
HAVING MIN(order_item_id) != 1 OR MAX(order_item_id) != COUNT(*)
ORDER BY item_count DESC;

PRINT 'Expected: order_item_id starts at 1 and increments by 1';

-- ============================================================================
-- 9. CHECK RELATIONSHIP TO ORDERS TABLE
-- ============================================================================
-- Verify all order_id exist in orders table

SELECT 
    COUNT(DISTINCT oi.order_id) AS total_order_ids,
    COUNT(DISTINCT o.order_id) AS matching_orders,
    COUNT(DISTINCT oi.order_id) - COUNT(DISTINCT o.order_id) AS unmatched_orders
FROM bronze.olist_order_items_dataset oi
LEFT JOIN bronze.olist_orders_dataset o 
    ON TRIM(REPLACE(oi.order_id, '"', '')) = TRIM(REPLACE(o.order_id, '"', ''));

-- ============================================================================
-- 10. CHECK RELATIONSHIP TO PRODUCTS TABLE
-- ============================================================================
-- Verify all product_id exist in products table

SELECT 
    COUNT(DISTINCT oi.product_id) AS total_product_ids,
    COUNT(DISTINCT p.product_id) AS matching_products,
    COUNT(DISTINCT oi.product_id) - COUNT(DISTINCT p.product_id) AS unmatched_products
FROM bronze.olist_order_items_dataset oi
LEFT JOIN bronze.olist_products_dataset p 
    ON TRIM(REPLACE(oi.product_id, '"', '')) = TRIM(REPLACE(p.product_id, '"', ''));

-- ============================================================================
-- 11. CHECK RELATIONSHIP TO SELLERS TABLE
-- ============================================================================
-- Verify all seller_id exist in sellers table

SELECT 
    COUNT(DISTINCT oi.seller_id) AS total_seller_ids,
    COUNT(DISTINCT s.seller_id) AS matching_sellers,
    COUNT(DISTINCT oi.seller_id) - COUNT(DISTINCT s.seller_id) AS unmatched_sellers
FROM bronze.olist_order_items_dataset oi
LEFT JOIN bronze.olist_sellers_dataset s 
    ON TRIM(REPLACE(oi.seller_id, '"', '')) = TRIM(REPLACE(s.seller_id, '"', ''));

-- ============================================================================
-- 12. ITEMS PER ORDER DISTRIBUTION
-- ============================================================================

SELECT 
    items_per_order,
    COUNT(*) AS order_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM bronze.olist_order_items_dataset) AS DECIMAL(5,2)) AS percentage
FROM (
    SELECT order_id, COUNT(*) AS items_per_order
    FROM bronze.olist_order_items_dataset
    GROUP BY order_id
) t
GROUP BY items_per_order
ORDER BY items_per_order;

-- ============================================================================
-- 13. TOP SELLERS BY ORDER ITEMS
-- ============================================================================

SELECT TOP 10
    seller_id,
    COUNT(*) AS items_sold,
    SUM(price) AS total_sales,
    AVG(price) AS avg_item_price
FROM bronze.olist_order_items_dataset
GROUP BY seller_id
ORDER BY items_sold DESC;

-- ============================================================================
-- 14. TOP PRODUCTS BY ORDER ITEMS
-- ============================================================================

SELECT TOP 10
    product_id,
    COUNT(*) AS times_ordered,
    SUM(price) AS total_revenue,
    AVG(price) AS avg_price
FROM bronze.olist_order_items_dataset
GROUP BY product_id
ORDER BY times_ordered DESC;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_line_items,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNT(DISTINCT seller_id) AS unique_sellers,
    AVG(price) AS avg_price,
    AVG(freight_value) AS avg_freight,
    SUM(price + freight_value) AS total_revenue
FROM bronze.olist_order_items_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Order Items';
PRINT 'Key Issues: Quotation marks, check date format, validate relationships';
PRINT '===============================================================================';


/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_geolocation_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_geolocation_dataset
Key Issues Expected: Duplicate ZIP codes, quotation marks, lowercase cities
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR DUPLICATE ZIP CODES
-- ============================================================================
-- Expectation: Many duplicates (same ZIP code with different coordinates)
-- This is a KNOWN issue that needs aggregation in Silver

SELECT 
    geolocation_zip_code_prefix,
    COUNT(*) AS duplicate_count,
    COUNT(DISTINCT geolocation_lat) AS unique_latitudes,
    COUNT(DISTINCT geolocation_lng) AS unique_longitudes
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

PRINT 'Note: Duplicate ZIP codes are EXPECTED - will be aggregated in Silver';

-- Show total duplicates vs unique
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT geolocation_zip_code_prefix) AS unique_zip_codes,
    COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix) AS duplicate_records
FROM bronze.olist_geolocation_dataset;

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================
-- Expectation: No results (all fields should be trimmed)

SELECT geolocation_zip_code_prefix
FROM bronze.olist_geolocation_dataset
WHERE geolocation_zip_code_prefix != TRIM(geolocation_zip_code_prefix);

SELECT geolocation_city
FROM bronze.olist_geolocation_dataset
WHERE geolocation_city != TRIM(geolocation_city);

SELECT geolocation_state
FROM bronze.olist_geolocation_dataset
WHERE geolocation_state != TRIM(geolocation_state);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================
-- Expectation: Quotation marks present (based on customer table pattern)

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN geolocation_zip_code_prefix LIKE '%"%' THEN 1 ELSE 0 END) AS zip_with_quotes,
    SUM(CASE WHEN geolocation_city LIKE '%"%' THEN 1 ELSE 0 END) AS city_with_quotes,
    SUM(CASE WHEN geolocation_state LIKE '%"%' THEN 1 ELSE 0 END) AS state_with_quotes
FROM bronze.olist_geolocation_dataset;

-- ============================================================================
-- 4. DATA STANDARDIZATION & CONSISTENCY - STATE CODES
-- ============================================================================
-- Check for invalid or inconsistent state codes

SELECT DISTINCT geolocation_state
FROM bronze.olist_geolocation_dataset
ORDER BY geolocation_state;

-- Identify invalid state codes
SELECT DISTINCT geolocation_state
FROM bronze.olist_geolocation_dataset
WHERE geolocation_state NOT IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
    'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
    'RS','RO','RR','SC','SP','SE','TO'
)
ORDER BY geolocation_state;

-- ============================================================================
-- 5. DATA STANDARDIZATION - CITY NAMES
-- ============================================================================
-- Check city name format (should be lowercase in Bronze)

SELECT DISTINCT 
    geolocation_city,
    LEN(geolocation_city) AS city_length
FROM bronze.olist_geolocation_dataset
WHERE geolocation_city LIKE '%[A-Z]%' -- Check for uppercase
ORDER BY geolocation_city;

PRINT 'Note: City names are expected to be lowercase in Bronze layer';

-- ============================================================================
-- 6. ZIP CODE FORMAT VALIDATION
-- ============================================================================
-- Check for invalid ZIP code formats
-- Expectation: All ZIP codes should be 5 digits

SELECT 
    geolocation_zip_code_prefix,
    LEN(geolocation_zip_code_prefix) AS zip_length,
    COUNT(*) AS count
FROM bronze.olist_geolocation_dataset
WHERE LEN(geolocation_zip_code_prefix) != 5
   OR geolocation_zip_code_prefix LIKE '%[^0-9]%'
GROUP BY geolocation_zip_code_prefix, LEN(geolocation_zip_code_prefix)
ORDER BY count DESC;

-- ============================================================================
-- 7. CHECK FOR NULL VALUES IN COORDINATES
-- ============================================================================
-- Expectation: Some NULL values may exist

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
    SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END) AS null_latitude,
    SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END) AS null_longitude,
    SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END) AS null_state
FROM bronze.olist_geolocation_dataset;

-- ============================================================================
-- 8. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN geolocation_zip_code_prefix = '' THEN 1 ELSE 0 END) AS empty_zip,
    SUM(CASE WHEN geolocation_city = '' THEN 1 ELSE 0 END) AS empty_city,
    SUM(CASE WHEN geolocation_state = '' THEN 1 ELSE 0 END) AS empty_state
FROM bronze.olist_geolocation_dataset;

-- ============================================================================
-- 9. CHECK FOR COORDINATE OUTLIERS
-- ============================================================================
-- Valid Brazilian coordinates:
-- Latitude: approximately -33 to +5
-- Longitude: approximately -74 to -30

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN geolocation_lat < -34 OR geolocation_lat > 6 THEN 1 ELSE 0 END) AS invalid_latitude,
    SUM(CASE WHEN geolocation_lng < -75 OR geolocation_lng > -29 THEN 1 ELSE 0 END) AS invalid_longitude,
    MIN(geolocation_lat) AS min_lat,
    MAX(geolocation_lat) AS max_lat,
    MIN(geolocation_lng) AS min_lng,
    MAX(geolocation_lng) AS max_lng
FROM bronze.olist_geolocation_dataset
WHERE geolocation_lat IS NOT NULL 
  AND geolocation_lng IS NOT NULL;

-- Show outlier records
SELECT 
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM bronze.olist_geolocation_dataset
WHERE geolocation_lat < -34 OR geolocation_lat > 6
   OR geolocation_lng < -75 OR geolocation_lng > -29;

-- ============================================================================
-- 10. CHECK COORDINATE VARIANCE PER ZIP CODE
-- ============================================================================
-- Show ZIP codes with high coordinate variance (data quality issue)

SELECT 
    geolocation_zip_code_prefix,
    COUNT(*) AS record_count,
    MIN(geolocation_lat) AS min_lat,
    MAX(geolocation_lat) AS max_lat,
    MAX(geolocation_lat) - MIN(geolocation_lat) AS lat_variance,
    MIN(geolocation_lng) AS min_lng,
    MAX(geolocation_lng) AS max_lng,
    MAX(geolocation_lng) - MIN(geolocation_lng) AS lng_variance
FROM bronze.olist_geolocation_dataset
WHERE geolocation_lat IS NOT NULL 
  AND geolocation_lng IS NOT NULL
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
   AND (MAX(geolocation_lat) - MIN(geolocation_lat) > 0.1
    OR MAX(geolocation_lng) - MIN(geolocation_lng) > 0.1)
ORDER BY lat_variance DESC;

-- ============================================================================
-- 11. DATA DISTRIBUTION BY STATE
-- ============================================================================

SELECT 
    geolocation_state,
    COUNT(*) AS total_records,
    COUNT(DISTINCT geolocation_zip_code_prefix) AS unique_zip_codes,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_state
ORDER BY total_records DESC;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_records,
    COUNT(DISTINCT geolocation_zip_code_prefix) AS unique_zip_codes,
    COUNT(DISTINCT geolocation_state) AS unique_states,
    COUNT(DISTINCT geolocation_city) AS unique_cities,
    AVG(geolocation_lat) AS avg_latitude,
    AVG(geolocation_lng) AS avg_longitude
FROM bronze.olist_geolocation_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Geolocation';
PRINT 'Key Issues: Duplicates, Quotation marks, Lowercase cities, Coordinate outliers';
PRINT '===============================================================================';

/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_orders_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_orders_dataset
Key Issues Expected: Quotation marks, VARCHAR dates, NULL dates, invalid date sequences
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY (order_id)
-- ============================================================================
-- Expectation: No results (all order_id should be unique and not null)

SELECT 
    order_id,
    COUNT(*) AS duplicate_count
FROM bronze.olist_orders_dataset
GROUP BY order_id
HAVING COUNT(*) > 1 OR order_id IS NULL;

PRINT 'Check 1: Primary key validation';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT order_id
FROM bronze.olist_orders_dataset
WHERE order_id != TRIM(order_id);

SELECT customer_id
FROM bronze.olist_orders_dataset
WHERE customer_id != TRIM(customer_id);

SELECT order_status
FROM bronze.olist_orders_dataset
WHERE order_status != TRIM(order_status);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================
-- Expectation: Quotation marks present (based on pattern from other tables)

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id LIKE '%"%' THEN 1 ELSE 0 END) AS order_id_with_quotes,
    SUM(CASE WHEN customer_id LIKE '%"%' THEN 1 ELSE 0 END) AS customer_id_with_quotes,
    SUM(CASE WHEN order_status LIKE '%"%' THEN 1 ELSE 0 END) AS status_with_quotes
FROM bronze.olist_orders_dataset;

-- ============================================================================
-- 4. DATA STANDARDIZATION - ORDER STATUS VALUES
-- ============================================================================
-- Check all order status values

SELECT DISTINCT order_status, COUNT(*) AS count
FROM bronze.olist_orders_dataset
GROUP BY order_status
ORDER BY count DESC;

PRINT 'Expected statuses: delivered, shipped, canceled, processing, unavailable, invoiced, created, approved';

-- ============================================================================
-- 5. CHECK DATE FIELD FORMATS (Stored as VARCHAR)
-- ============================================================================
-- All date fields are VARCHAR - need to check format and convertibility

-- Check purchase timestamp
SELECT TOP 10
    order_purchase_timestamp,
    LEN(order_purchase_timestamp) AS length
FROM bronze.olist_orders_dataset
WHERE order_purchase_timestamp IS NOT NULL
ORDER BY order_purchase_timestamp;

-- Check approved timestamp
SELECT TOP 10
    order_approved_at,
    LEN(order_approved_at) AS length
FROM bronze.olist_orders_dataset
WHERE order_approved_at IS NOT NULL
ORDER BY order_approved_at;

-- Check delivery carrier date
SELECT TOP 10
    order_delivered_carrier_date,
    LEN(order_delivered_carrier_date) AS length
FROM bronze.olist_orders_dataset
WHERE order_delivered_carrier_date IS NOT NULL
ORDER BY order_delivered_carrier_date;

-- ============================================================================
-- 6. CHECK FOR NULL DATE VALUES
-- ============================================================================
-- Some dates may be NULL for canceled/processing orders

SELECT 
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) AS null_purchase,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) AS null_approved,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END) AS null_shipped,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered,
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS null_estimated
FROM bronze.olist_orders_dataset;

-- ============================================================================
-- 7. CHECK DATE SEQUENCE LOGIC
-- ============================================================================
-- Dates should follow: purchase <= approved <= shipped <= delivered

-- This check will fail because dates are VARCHAR - we'll fix in Silver
SELECT 
    'Date Sequence Issues' AS issue_type,
    COUNT(*) AS count
FROM bronze.olist_orders_dataset
WHERE 
    -- Can't compare VARCHAR dates directly - will handle in transformation
    order_purchase_timestamp > order_approved_at
    OR order_approved_at > order_delivered_carrier_date
    OR order_delivered_carrier_date > order_delivered_customer_date;

PRINT 'Note: Date sequence check cannot be performed on VARCHAR dates';

-- ============================================================================
-- 8. CHECK RELATIONSHIP TO CUSTOMERS TABLE
-- ============================================================================
-- Verify all customer_id exist in customers table

SELECT 
    COUNT(DISTINCT o.customer_id) AS total_order_customers,
    COUNT(DISTINCT c.customer_id) AS matching_customers,
    COUNT(DISTINCT o.customer_id) - COUNT(DISTINCT c.customer_id) AS unmatched_customers
FROM bronze.olist_orders_dataset o
LEFT JOIN bronze.olist_customers_dataset c 
    ON TRIM(REPLACE(o.customer_id, '"', '')) = TRIM(REPLACE(c.customer_id, '"', ''));

-- Show sample of unmatched customers (if any)
SELECT DISTINCT TOP 10
    TRIM(REPLACE(o.customer_id, '"', '')) AS order_customer_id
FROM bronze.olist_orders_dataset o
LEFT JOIN bronze.olist_customers_dataset c 
    ON TRIM(REPLACE(o.customer_id, '"', '')) = TRIM(REPLACE(c.customer_id, '"', ''))
WHERE c.customer_id IS NULL;

-- ============================================================================
-- 9. ORDER STATUS DISTRIBUTION
-- ============================================================================

SELECT 
    order_status,
    COUNT(*) AS order_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_orders_dataset
GROUP BY order_status
ORDER BY order_count DESC;

-- ============================================================================
-- 10. CHECK FOR INVALID DATE FORMATS
-- ============================================================================
-- Check if dates can be converted to DATETIME

-- Count unconvertible purchase timestamps
SELECT COUNT(*) AS invalid_purchase_timestamps
FROM bronze.olist_orders_dataset
WHERE order_purchase_timestamp IS NOT NULL
  AND TRY_CONVERT(DATETIME2, order_purchase_timestamp) IS NULL;

-- Count unconvertible approved timestamps
SELECT COUNT(*) AS invalid_approved_timestamps
FROM bronze.olist_orders_dataset
WHERE order_approved_at IS NOT NULL
  AND TRY_CONVERT(DATETIME2, order_approved_at) IS NULL;

-- Show sample of invalid dates (if any)
SELECT TOP 10
    order_id,
    order_purchase_timestamp,
    order_approved_at
FROM bronze.olist_orders_dataset
WHERE TRY_CONVERT(DATETIME2, order_purchase_timestamp) IS NULL
   OR TRY_CONVERT(DATETIME2, order_approved_at) IS NULL;

-- ============================================================================
-- 11. CHECK FOR OUT OF RANGE DATES
-- ============================================================================
-- Orders should be between 2016-2018 based on Olist dataset timeframe

SELECT 
    MIN(TRY_CONVERT(DATETIME2, order_purchase_timestamp)) AS earliest_order,
    MAX(TRY_CONVERT(DATETIME2, order_purchase_timestamp)) AS latest_order,
    DATEDIFF(DAY, 
        MIN(TRY_CONVERT(DATETIME2, order_purchase_timestamp)),
        MAX(TRY_CONVERT(DATETIME2, order_purchase_timestamp))
    ) AS days_span
FROM bronze.olist_orders_dataset
WHERE TRY_CONVERT(DATETIME2, order_purchase_timestamp) IS NOT NULL;

-- ============================================================================
-- 12. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN order_id = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN customer_id = '' THEN 1 ELSE 0 END) AS empty_customer_id,
    SUM(CASE WHEN order_status = '' THEN 1 ELSE 0 END) AS empty_status
FROM bronze.olist_orders_dataset;

-- ============================================================================
-- 13. ORDERS BY YEAR AND MONTH
-- ============================================================================

SELECT 
    YEAR(TRY_CONVERT(DATETIME2, order_purchase_timestamp)) AS order_year,
    MONTH(TRY_CONVERT(DATETIME2, order_purchase_timestamp)) AS order_month,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset
WHERE TRY_CONVERT(DATETIME2, order_purchase_timestamp) IS NOT NULL
GROUP BY 
    YEAR(TRY_CONVERT(DATETIME2, order_purchase_timestamp)),
    MONTH(TRY_CONVERT(DATETIME2, order_purchase_timestamp))
ORDER BY order_year, order_month;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_order_ids,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT order_status) AS unique_statuses
FROM bronze.olist_orders_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Orders';
PRINT 'Key Issues: Quotation marks, VARCHAR dates, NULL dates for some statuses';
PRINT '===============================================================================';


/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_products_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_products_dataset
Key Issues Expected: Quotation marks, NULL values, invalid dimensions, negative values
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- ============================================================================
-- Primary Key: product_id
-- Expectation: No duplicates, no NULLs

SELECT 
    product_id,
    COUNT(*) AS duplicate_count
FROM bronze.olist_products_dataset
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;

PRINT 'Check 1: Primary key validation - product_id';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT product_id
FROM bronze.olist_products_dataset
WHERE product_id != TRIM(product_id);

SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name != TRIM(product_category_name);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_id LIKE '%"%' THEN 1 ELSE 0 END) AS product_id_with_quotes,
    SUM(CASE WHEN product_category_name LIKE '%"%' THEN 1 ELSE 0 END) AS category_with_quotes
FROM bronze.olist_products_dataset;

-- ============================================================================
-- 4. CHECK FOR NULL VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) AS null_category_name,
    SUM(CASE WHEN product_name_lenght IS NULL THEN 1 ELSE 0 END) AS null_name_length,
    SUM(CASE WHEN product_description_lenght IS NULL THEN 1 ELSE 0 END) AS null_desc_length,
    SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) AS null_photos_qty,
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) AS null_weight,
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) AS null_length,
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) AS null_height,
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) AS null_width
FROM bronze.olist_products_dataset;

-- ============================================================================
-- 5. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN product_id = '' THEN 1 ELSE 0 END) AS empty_product_id,
    SUM(CASE WHEN product_category_name = '' THEN 1 ELSE 0 END) AS empty_category_name
FROM bronze.olist_products_dataset;

-- ============================================================================
-- 6. VALIDATE NUMERIC VALUES - CHECK FOR NEGATIVE OR ZERO VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    -- Negative values
    SUM(CASE WHEN product_name_lenght < 0 THEN 1 ELSE 0 END) AS negative_name_length,
    SUM(CASE WHEN product_description_lenght < 0 THEN 1 ELSE 0 END) AS negative_desc_length,
    SUM(CASE WHEN product_photos_qty < 0 THEN 1 ELSE 0 END) AS negative_photos,
    SUM(CASE WHEN product_weight_g < 0 THEN 1 ELSE 0 END) AS negative_weight,
    SUM(CASE WHEN product_length_cm < 0 THEN 1 ELSE 0 END) AS negative_length,
    SUM(CASE WHEN product_height_cm < 0 THEN 1 ELSE 0 END) AS negative_height,
    SUM(CASE WHEN product_width_cm < 0 THEN 1 ELSE 0 END) AS negative_width,
    -- Zero values (may be valid for some fields)
    SUM(CASE WHEN product_weight_g = 0 THEN 1 ELSE 0 END) AS zero_weight,
    SUM(CASE WHEN product_length_cm = 0 THEN 1 ELSE 0 END) AS zero_length,
    SUM(CASE WHEN product_height_cm = 0 THEN 1 ELSE 0 END) AS zero_height,
    SUM(CASE WHEN product_width_cm = 0 THEN 1 ELSE 0 END) AS zero_width,
    SUM(CASE WHEN product_photos_qty = 0 THEN 1 ELSE 0 END) AS zero_photos
FROM bronze.olist_products_dataset;

-- Show sample records with problematic numeric values
SELECT TOP 10
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_weight_g <= 0 
   OR product_length_cm <= 0 
   OR product_height_cm <= 0 
   OR product_width_cm <= 0
ORDER BY product_id;

-- ============================================================================
-- 7. PRODUCT CATEGORY ANALYSIS
-- ============================================================================
-- Check unique categories (Portuguese names)

SELECT 
    product_category_name,
    COUNT(*) AS product_count,
    AVG(product_weight_g) AS avg_weight,
    AVG(product_photos_qty) AS avg_photos,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_products_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_products_dataset
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY product_count DESC;

PRINT 'Product categories in Portuguese';

-- ============================================================================
-- 8. PRODUCT DIMENSIONS ANALYSIS
-- ============================================================================
-- Check dimension statistics

SELECT 
    'Dimension Stats' AS metric,
    MIN(product_weight_g) AS min_weight,
    MAX(product_weight_g) AS max_weight,
    AVG(product_weight_g) AS avg_weight,
    MIN(product_length_cm) AS min_length,
    MAX(product_length_cm) AS max_length,
    AVG(product_length_cm) AS avg_length,
    MIN(product_height_cm) AS min_height,
    MAX(product_height_cm) AS max_height,
    AVG(product_height_cm) AS avg_height,
    MIN(product_width_cm) AS min_width,
    MAX(product_width_cm) AS max_width,
    AVG(product_width_cm) AS avg_width
FROM bronze.olist_products_dataset
WHERE product_weight_g IS NOT NULL
  AND product_length_cm IS NOT NULL
  AND product_height_cm IS NOT NULL
  AND product_width_cm IS NOT NULL;

-- ============================================================================
-- 9. PRODUCT PHOTOS ANALYSIS
-- ============================================================================

SELECT 
    product_photos_qty,
    COUNT(*) AS product_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_products_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_products_dataset
WHERE product_photos_qty IS NOT NULL
GROUP BY product_photos_qty
ORDER BY product_photos_qty;

PRINT 'Distribution of product photos';

-- Products without photos
SELECT 
    COUNT(*) AS products_without_photos,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_products_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_products_dataset
WHERE product_photos_qty = 0 OR product_photos_qty IS NULL;

-- ============================================================================
-- 10. NAME AND DESCRIPTION LENGTH ANALYSIS
-- ============================================================================

SELECT 
    'Text Length Stats' AS metric,
    MIN(product_name_lenght) AS min_name_length,
    MAX(product_name_lenght) AS max_name_length,
    AVG(product_name_lenght) AS avg_name_length,
    MIN(product_description_lenght) AS min_desc_length,
    MAX(product_description_lenght) AS max_desc_length,
    AVG(product_description_lenght) AS avg_desc_length
FROM bronze.olist_products_dataset
WHERE product_name_lenght IS NOT NULL
  AND product_description_lenght IS NOT NULL;

-- Products with unusually short or long names/descriptions
SELECT TOP 10
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    CASE 
        WHEN product_name_lenght < 10 THEN 'Very short name'
        WHEN product_name_lenght > 100 THEN 'Very long name'
        WHEN product_description_lenght < 50 THEN 'Very short description'
        WHEN product_description_lenght > 2000 THEN 'Very long description'
    END AS issue
FROM bronze.olist_products_dataset
WHERE product_name_lenght < 10 
   OR product_name_lenght > 100
   OR product_description_lenght < 50
   OR product_description_lenght > 2000
ORDER BY product_name_lenght DESC;

-- ============================================================================
-- 11. CHECK RELATIONSHIP TO ORDER ITEMS TABLE
-- ============================================================================
-- Products that don't appear in any orders (orphaned products)

SELECT 
    COUNT(DISTINCT p.product_id) AS total_products,
    COUNT(DISTINCT oi.product_id) AS products_in_orders,
    COUNT(DISTINCT p.product_id) - COUNT(DISTINCT oi.product_id) AS orphaned_products
FROM bronze.olist_products_dataset p
LEFT JOIN bronze.olist_order_items_dataset oi 
    ON TRIM(REPLACE(p.product_id, '"', '')) = TRIM(REPLACE(oi.product_id, '"', ''));

-- Show sample of orphaned products
SELECT DISTINCT TOP 10
    TRIM(REPLACE(p.product_id, '"', '')) AS product_id,
    p.product_category_name
FROM bronze.olist_products_dataset p
LEFT JOIN bronze.olist_order_items_dataset oi 
    ON TRIM(REPLACE(p.product_id, '"', '')) = TRIM(REPLACE(oi.product_id, '"', ''))
WHERE oi.product_id IS NULL;

-- ============================================================================
-- 12. VOLUME CALCULATION CHECK
-- ============================================================================
-- Calculate volume and check for extreme values

SELECT TOP 20
    product_id,
    product_category_name,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    (product_length_cm * product_height_cm * product_width_cm) AS volume_cm3,
    product_weight_g
FROM bronze.olist_products_dataset
WHERE product_length_cm IS NOT NULL
  AND product_height_cm IS NOT NULL
  AND product_width_cm IS NOT NULL
  AND product_length_cm > 0
  AND product_height_cm > 0
  AND product_width_cm > 0
ORDER BY (product_length_cm * product_height_cm * product_width_cm) DESC;

PRINT 'Products with largest volume';

-- ============================================================================
-- 13. DATA COMPLETENESS CHECK
-- ============================================================================
-- Records with all fields populated vs missing data

SELECT 
    'Data Completeness' AS metric,
    COUNT(*) AS total_records,
    SUM(CASE 
        WHEN product_id IS NOT NULL 
        AND product_category_name IS NOT NULL
        AND product_name_lenght IS NOT NULL
        AND product_description_lenght IS NOT NULL
        AND product_photos_qty IS NOT NULL
        AND product_weight_g IS NOT NULL
        AND product_length_cm IS NOT NULL
        AND product_height_cm IS NOT NULL
        AND product_width_cm IS NOT NULL
        THEN 1 ELSE 0 
    END) AS complete_records,
    SUM(CASE 
        WHEN product_id IS NULL 
        OR product_category_name IS NULL
        OR product_name_lenght IS NULL
        OR product_description_lenght IS NULL
        OR product_photos_qty IS NULL
        OR product_weight_g IS NULL
        OR product_length_cm IS NULL
        OR product_height_cm IS NULL
        OR product_width_cm IS NULL
        THEN 1 ELSE 0 
    END) AS incomplete_records,
    CAST(SUM(CASE 
        WHEN product_id IS NOT NULL 
        AND product_category_name IS NOT NULL
        AND product_name_lenght IS NOT NULL
        AND product_description_lenght IS NOT NULL
        AND product_photos_qty IS NOT NULL
        AND product_weight_g IS NOT NULL
        AND product_length_cm IS NOT NULL
        AND product_height_cm IS NOT NULL
        AND product_width_cm IS NOT NULL
        THEN 1 ELSE 0 
    END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_complete
FROM bronze.olist_products_dataset;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_products,
    COUNT(DISTINCT product_id) AS unique_product_ids,
    COUNT(DISTINCT product_category_name) AS unique_categories,
    AVG(product_weight_g) AS avg_weight_g,
    AVG(product_photos_qty) AS avg_photos
FROM bronze.olist_products_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Products Dataset';
PRINT 'Key Issues: Quotation marks, NULL values, negative/zero dimensions, orphaned products';
PRINT '===============================================================================';

/*
===============================================================================
BRONZE DATA QUALITY CHECK - olist_sellers_dataset
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.olist_sellers_dataset
Key Issues Expected: Quotation marks, NULL values, geographic data validation
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- ============================================================================
-- Primary Key: seller_id
-- Expectation: No duplicates, no NULLs

SELECT 
    seller_id,
    COUNT(*) AS duplicate_count
FROM bronze.olist_sellers_dataset
GROUP BY seller_id
HAVING COUNT(*) > 1 OR seller_id IS NULL;

PRINT 'Check 1: Primary key validation - seller_id';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT seller_id
FROM bronze.olist_sellers_dataset
WHERE seller_id != TRIM(seller_id);

SELECT seller_zip_code_prefix
FROM bronze.olist_sellers_dataset
WHERE seller_zip_code_prefix != TRIM(seller_zip_code_prefix);

SELECT seller_city
FROM bronze.olist_sellers_dataset
WHERE seller_city != TRIM(seller_city);

SELECT seller_state
FROM bronze.olist_sellers_dataset
WHERE seller_state != TRIM(seller_state);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN seller_id LIKE '%"%' THEN 1 ELSE 0 END) AS seller_id_with_quotes,
    SUM(CASE WHEN seller_zip_code_prefix LIKE '%"%' THEN 1 ELSE 0 END) AS zip_with_quotes,
    SUM(CASE WHEN seller_city LIKE '%"%' THEN 1 ELSE 0 END) AS city_with_quotes,
    SUM(CASE WHEN seller_state LIKE '%"%' THEN 1 ELSE 0 END) AS state_with_quotes
FROM bronze.olist_sellers_dataset;

-- ============================================================================
-- 4. CHECK FOR NULL VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code,
    SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS null_state
FROM bronze.olist_sellers_dataset;

-- ============================================================================
-- 5. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN seller_id = '' THEN 1 ELSE 0 END) AS empty_seller_id,
    SUM(CASE WHEN seller_zip_code_prefix = '' THEN 1 ELSE 0 END) AS empty_zip_code,
    SUM(CASE WHEN seller_city = '' THEN 1 ELSE 0 END) AS empty_city,
    SUM(CASE WHEN seller_state = '' THEN 1 ELSE 0 END) AS empty_state
FROM bronze.olist_sellers_dataset;

-- ============================================================================
-- 6. VALIDATE ZIP CODE FORMAT
-- ============================================================================
-- Brazilian ZIP codes should be 5 digits (prefix format)

SELECT 
    COUNT(*) AS total_records,
    MIN(LEN(seller_zip_code_prefix)) AS min_zip_length,
    MAX(LEN(seller_zip_code_prefix)) AS max_zip_length,
    AVG(LEN(seller_zip_code_prefix)) AS avg_zip_length,
    SUM(CASE WHEN LEN(seller_zip_code_prefix) != 5 THEN 1 ELSE 0 END) AS invalid_zip_length
FROM bronze.olist_sellers_dataset
WHERE seller_zip_code_prefix IS NOT NULL;

-- Show sample of invalid ZIP codes
SELECT TOP 10
    seller_id,
    seller_zip_code_prefix,
    LEN(seller_zip_code_prefix) AS zip_length,
    seller_city,
    seller_state
FROM bronze.olist_sellers_dataset
WHERE LEN(seller_zip_code_prefix) != 5
ORDER BY LEN(seller_zip_code_prefix);

-- Check if ZIP codes are numeric
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE 
        WHEN seller_zip_code_prefix IS NOT NULL 
        AND ISNUMERIC(seller_zip_code_prefix) = 0 
        THEN 1 ELSE 0 
    END) AS non_numeric_zip_codes
FROM bronze.olist_sellers_dataset;

-- ============================================================================
-- 7. VALIDATE STATE CODES
-- ============================================================================
-- Brazilian states should be 2-letter codes (AC, AL, AM, AP, BA, CE, DF, ES, GO, MA, MG, MS, MT, PA, PB, PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP, TO)

SELECT 
    seller_state,
    COUNT(*) AS seller_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_sellers_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_sellers_dataset
WHERE seller_state IS NOT NULL
GROUP BY seller_state
ORDER BY seller_count DESC;

PRINT 'Expected: 27 Brazilian state codes (2 letters each)';

-- Check for invalid state code lengths
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN LEN(seller_state) != 2 THEN 1 ELSE 0 END) AS invalid_state_length
FROM bronze.olist_sellers_dataset
WHERE seller_state IS NOT NULL;

-- Show sellers with invalid state codes
SELECT TOP 10
    seller_id,
    seller_state,
    LEN(seller_state) AS state_length,
    seller_city
FROM bronze.olist_sellers_dataset
WHERE LEN(seller_state) != 2;

-- ============================================================================
-- 8. CITY NAME ANALYSIS
-- ============================================================================
-- Check unique cities and case sensitivity

SELECT 
    COUNT(DISTINCT seller_city) AS unique_cities,
    COUNT(DISTINCT LOWER(seller_city)) AS unique_cities_lowercase
FROM bronze.olist_sellers_dataset
WHERE seller_city IS NOT NULL;

-- Most common seller cities
SELECT TOP 20
    seller_city,
    seller_state,
    COUNT(*) AS seller_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_sellers_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_sellers_dataset
WHERE seller_city IS NOT NULL
GROUP BY seller_city, seller_state
ORDER BY seller_count DESC;

PRINT 'Top 20 seller cities';

-- ============================================================================
-- 9. GEOGRAPHIC DISTRIBUTION
-- ============================================================================
-- Sellers by state

SELECT 
    seller_state,
    COUNT(*) AS seller_count,
    COUNT(DISTINCT seller_city) AS cities_count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.olist_sellers_dataset) AS DECIMAL(5,2)) AS percentage
FROM bronze.olist_sellers_dataset
WHERE seller_state IS NOT NULL
GROUP BY seller_state
ORDER BY seller_count DESC;

PRINT 'Geographic distribution of sellers by state';

-- ============================================================================
-- 10. CHECK RELATIONSHIP TO ORDER ITEMS TABLE
-- ============================================================================
-- Sellers that don't appear in any orders (orphaned sellers)

SELECT 
    COUNT(DISTINCT s.seller_id) AS total_sellers,
    COUNT(DISTINCT oi.seller_id) AS sellers_in_orders,
    COUNT(DISTINCT s.seller_id) - COUNT(DISTINCT oi.seller_id) AS orphaned_sellers
FROM bronze.olist_sellers_dataset s
LEFT JOIN bronze.olist_order_items_dataset oi 
    ON TRIM(REPLACE(s.seller_id, '"', '')) = TRIM(REPLACE(oi.seller_id, '"', ''));

-- Show sample of orphaned sellers
SELECT DISTINCT TOP 10
    TRIM(REPLACE(s.seller_id, '"', '')) AS seller_id,
    s.seller_city,
    s.seller_state
FROM bronze.olist_sellers_dataset s
LEFT JOIN bronze.olist_order_items_dataset oi 
    ON TRIM(REPLACE(s.seller_id, '"', '')) = TRIM(REPLACE(oi.seller_id, '"', ''))
WHERE oi.seller_id IS NULL;

-- ============================================================================
-- 11. CHECK FOR DUPLICATE LOCATIONS
-- ============================================================================
-- Multiple sellers at same location

SELECT 
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    COUNT(*) AS seller_count
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix, seller_city, seller_state
HAVING COUNT(*) > 5
ORDER BY seller_count DESC;

PRINT 'Locations with multiple sellers (potential marketplaces)';

-- ============================================================================
-- 12. CASE SENSITIVITY CHECK
-- ============================================================================
-- Check if cities/states have inconsistent casing
SELECT TOP 20
    LOWER(seller_city) AS city_lowercase,
    COUNT(DISTINCT seller_city) AS different_casings,
    COUNT(*) AS seller_count
FROM bronze.olist_sellers_dataset
WHERE seller_city IS NOT NULL
GROUP BY LOWER(seller_city)
HAVING COUNT(DISTINCT seller_city) > 1
ORDER BY different_casings DESC;

PRINT 'Cities with inconsistent casing (e.g., "Sao Paulo" vs "SAO PAULO")';

-- ============================================================================
-- 13. DATA COMPLETENESS CHECK
-- ============================================================================

SELECT 
    'Data Completeness' AS metric,
    COUNT(*) AS total_records,
    SUM(CASE 
        WHEN seller_id IS NOT NULL 
        AND seller_zip_code_prefix IS NOT NULL
        AND seller_city IS NOT NULL
        AND seller_state IS NOT NULL
        THEN 1 ELSE 0 
    END) AS complete_records,
    SUM(CASE 
        WHEN seller_id IS NULL 
        OR seller_zip_code_prefix IS NULL
        OR seller_city IS NULL
        OR seller_state IS NULL
        THEN 1 ELSE 0 
    END) AS incomplete_records,
    CAST(SUM(CASE 
        WHEN seller_id IS NOT NULL 
        AND seller_zip_code_prefix IS NOT NULL
        AND seller_city IS NOT NULL
        AND seller_state IS NOT NULL
        THEN 1 ELSE 0 
    END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_complete
FROM bronze.olist_sellers_dataset;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_sellers,
    COUNT(DISTINCT seller_id) AS unique_seller_ids,
    COUNT(DISTINCT seller_state) AS unique_states,
    COUNT(DISTINCT seller_city) AS unique_cities,
    COUNT(DISTINCT seller_zip_code_prefix) AS unique_zip_codes
FROM bronze.olist_sellers_dataset;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Sellers Dataset';
PRINT 'Key Issues: Quotation marks, ZIP code validation, state codes, case sensitivity';
PRINT '===============================================================================';

/*
===============================================================================
BRONZE DATA QUALITY CHECK - product_category_name_translation
===============================================================================
Purpose: Identify data quality issues in the Bronze layer before transformation
Table: bronze.product_category_name_translation
Key Issues Expected: Quotation marks, NULL values, missing translations
Note: This is a small reference/lookup table
===============================================================================
*/

-- ============================================================================
-- 1. CHECK FOR NULLS OR DUPLICATES IN PRIMARY KEY
-- ============================================================================
-- Primary Key: product_category_name (Portuguese)
-- Expectation: No duplicates, no NULLs

SELECT 
    product_category_name,
    COUNT(*) AS duplicate_count
FROM bronze.product_category_name_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1 OR product_category_name IS NULL;

PRINT 'Check 1: Primary key validation - product_category_name';

-- Check for duplicate English translations
SELECT 
    product_category_name_english,
    COUNT(*) AS duplicate_count,
    STRING_AGG(product_category_name, ', ') AS portuguese_variations
FROM bronze.product_category_name_translation
GROUP BY product_category_name_english
HAVING COUNT(*) > 1;

PRINT 'Check: Multiple Portuguese categories mapping to same English translation';

-- ============================================================================
-- 2. CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- ============================================================================

SELECT product_category_name
FROM bronze.product_category_name_translation
WHERE product_category_name != TRIM(product_category_name);

SELECT product_category_name_english
FROM bronze.product_category_name_translation
WHERE product_category_name_english != TRIM(product_category_name_english);

-- ============================================================================
-- 3. CHECK FOR QUOTATION MARKS
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_category_name LIKE '%"%' THEN 1 ELSE 0 END) AS portuguese_with_quotes,
    SUM(CASE WHEN product_category_name_english LIKE '%"%' THEN 1 ELSE 0 END) AS english_with_quotes
FROM bronze.product_category_name_translation;

-- ============================================================================
-- 4. CHECK FOR NULL VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) AS null_portuguese,
    SUM(CASE WHEN product_category_name_english IS NULL THEN 1 ELSE 0 END) AS null_english
FROM bronze.product_category_name_translation;

-- ============================================================================
-- 5. CHECK FOR EMPTY STRING VALUES
-- ============================================================================

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN product_category_name = '' THEN 1 ELSE 0 END) AS empty_portuguese,
    SUM(CASE WHEN product_category_name_english = '' THEN 1 ELSE 0 END) AS empty_english
FROM bronze.product_category_name_translation;

-- ============================================================================
-- 6. VALIDATE UNDERSCORE FORMAT
-- ============================================================================
-- Both Portuguese and English should use underscores (e.g., "cama_mesa_banho", "bed_bath_table")

SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_category_name LIKE '% %' THEN 1 ELSE 0 END) AS portuguese_with_spaces,
    SUM(CASE WHEN product_category_name_english LIKE '% %' THEN 1 ELSE 0 END) AS english_with_spaces
FROM bronze.product_category_name_translation;

PRINT 'Expected: Categories use underscores, not spaces (e.g., "bed_bath_table" not "bed bath table")';

-- ============================================================================
-- 7. CHECK RELATIONSHIP TO PRODUCTS TABLE
-- ============================================================================
-- Categories in products that don't have translations

SELECT 
    COUNT(DISTINCT p.product_category_name) AS categories_in_products,
    COUNT(DISTINCT t.product_category_name) AS categories_with_translation,
    COUNT(DISTINCT p.product_category_name) - COUNT(DISTINCT t.product_category_name) AS missing_translations
FROM bronze.olist_products_dataset p
LEFT JOIN bronze.product_category_name_translation t 
    ON TRIM(REPLACE(LOWER(p.product_category_name), '"', '')) = TRIM(REPLACE(LOWER(t.product_category_name), '"', ''));

-- Show categories without translations
SELECT DISTINCT TOP 10
    TRIM(REPLACE(p.product_category_name, '"', '')) AS category_without_translation,
    COUNT(*) AS product_count
FROM bronze.olist_products_dataset p
LEFT JOIN bronze.product_category_name_translation t 
    ON TRIM(REPLACE(LOWER(p.product_category_name), '"', '')) = TRIM(REPLACE(LOWER(t.product_category_name), '"', ''))
WHERE t.product_category_name IS NULL
  AND p.product_category_name IS NOT NULL
GROUP BY TRIM(REPLACE(p.product_category_name, '"', ''))
ORDER BY product_count DESC;

PRINT 'Categories in products without English translation';

-- ============================================================================
-- 8. CHECK FOR ORPHANED TRANSLATIONS
-- ============================================================================
-- Translations that don't match any product category

SELECT 
    COUNT(DISTINCT t.product_category_name) AS total_translations,
    COUNT(DISTINCT p.product_category_name) AS matching_products,
    COUNT(DISTINCT t.product_category_name) - COUNT(DISTINCT p.product_category_name) AS orphaned_translations
FROM bronze.product_category_name_translation t
LEFT JOIN bronze.olist_products_dataset p 
    ON TRIM(REPLACE(LOWER(t.product_category_name), '"', '')) = TRIM(REPLACE(LOWER(p.product_category_name), '"', ''));

-- Show orphaned translations
SELECT 
    t.product_category_name,
    t.product_category_name_english
FROM bronze.product_category_name_translation t
LEFT JOIN bronze.olist_products_dataset p 
    ON TRIM(REPLACE(LOWER(t.product_category_name), '"', '')) = TRIM(REPLACE(LOWER(p.product_category_name), '"', ''))
WHERE p.product_category_name IS NULL;

PRINT 'Translations without matching products (might be valid for future use)';

-- ============================================================================
-- 9. CASE SENSITIVITY CHECK
-- ============================================================================

SELECT 
    LOWER(product_category_name) AS category_lowercase,
    COUNT(DISTINCT product_category_name) AS different_casings,
    STRING_AGG(product_category_name, ', ') AS variations
FROM bronze.product_category_name_translation
GROUP BY LOWER(product_category_name)
HAVING COUNT(DISTINCT product_category_name) > 1;

PRINT 'Categories with inconsistent casing';

-- ============================================================================
-- 10. TRANSLATION COMPLETENESS
-- ============================================================================

SELECT 
    'Translation Completeness' AS metric,
    COUNT(*) AS total_records,
    SUM(CASE 
        WHEN product_category_name IS NOT NULL 
        AND product_category_name_english IS NOT NULL
        THEN 1 ELSE 0 
    END) AS complete_translations,
    SUM(CASE 
        WHEN product_category_name IS NULL 
        OR product_category_name_english IS NULL
        THEN 1 ELSE 0 
    END) AS incomplete_translations,
    CAST(SUM(CASE 
        WHEN product_category_name IS NOT NULL 
        AND product_category_name_english IS NOT NULL
        THEN 1 ELSE 0 
    END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS pct_complete
FROM bronze.product_category_name_translation;

-- ============================================================================
-- 11. SHOW ALL TRANSLATIONS
-- ============================================================================

SELECT 
    product_category_name AS portuguese,
    product_category_name_english AS english
FROM bronze.product_category_name_translation
ORDER BY product_category_name;

PRINT 'All category translations (Portuguese → English)';

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT 
    'BRONZE LAYER SUMMARY' AS check_type,
    COUNT(*) AS total_translations,
    COUNT(DISTINCT product_category_name) AS unique_portuguese,
    COUNT(DISTINCT product_category_name_english) AS unique_english
FROM bronze.product_category_name_translation;

PRINT '===============================================================================';
PRINT 'Bronze Data Quality Check Complete - Category Translation';
PRINT 'Key Issues: Quotation marks, missing translations, orphaned translations';
PRINT '===============================================================================';
