# Data Catalog for Olist Gold Layer

## Overview
The Gold Layer is the business-level data representation for the Olist E-commerce Data Warehouse, structured to support analytical and reporting use cases. It consists of **4 dimension tables** and **1 fact table** following the **Star Schema** design pattern.

**Data Source:** Brazilian E-commerce Public Dataset by Olist  
**Time Period:** 2016-2018  
**Architecture:** Medallion Architecture (Bronze → Silver → Gold)  
**Database:** Olist_DataWarehouse  
**Schema:** gold

---

## Dimension Tables

### 1. **gold.dim_customer**
- **Purpose:** Stores customer details enriched with geographic and geolocation data for Brazilian customers.
- **Grain:** One row per unique customer_id
- **Row Count:** ~99,441 customers
- **Source:** silver.olist_customers_dataset + silver.olist_geolocation_dataset
- **Columns:**

| Column Name          | Data Type     | Description                                                                                   | Nullable | Sample Values |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| customer_key        | INT           | Surrogate key uniquely identifying each customer record in the dimension table. Primary Key.  | No       | 1, 2, 3, 4... |
| customer_id         | VARCHAR(255)  | Business key - unique identifier for each customer from source system.                        | No       | '06b8999e2fba1a1fbc88172c00ba8bc7' |
| customer_unique_id  | VARCHAR(255)  | Unique customer identifier across multiple orders (tracks repeat customers).                  | No       | '861eff4711a542e4b93843c6dd7febb0' |
| zip_code            | VARCHAR(10)   | Customer's postal code (CEP in Brazil). First 5 digits of full postal code.                  | Yes      | '14409', '01310', '13023' |
| city                | VARCHAR(100)  | Customer's city of residence in Brazil.                                                       | Yes      | 'sao paulo', 'rio de janeiro', 'brasilia' |
| state               | VARCHAR(2)    | Customer's state abbreviation (Brazilian states - 2 letter code).                            | Yes      | 'SP', 'RJ', 'MG', 'RS' |
| latitude            | DECIMAL(10,8) | Geographic latitude of customer location (enriched from geolocation data).                    | Yes      | -23.5489, -22.9068 |
| longitude           | DECIMAL(11,8) | Geographic longitude of customer location (enriched from geolocation data).                   | Yes      | -46.6388, -43.1729 |
| create_date         | DATETIME      | Timestamp when the customer record was created in the data warehouse.                         | No       | '2024-12-27 10:30:00' |

**Special Records:**
- customer_key = -1: "UNKNOWN" customer record for orders with missing customer references

---

### 2. **gold.dim_product**
- **Purpose:** Provides comprehensive information about products sold on the Olist marketplace, including categories and physical dimensions.
- **Grain:** One row per unique product_id
- **Row Count:** ~32,951 products
- **Source:** silver.olist_products_dataset + silver.product_category_name_translation
- **Columns:**

| Column Name            | Data Type     | Description                                                                                   | Nullable | Sample Values |
|-----------------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| product_key           | INT           | Surrogate key uniquely identifying each product record in the dimension table. Primary Key.   | No       | 1, 2, 3, 4... |
| product_id            | VARCHAR(255)  | Business key - unique identifier for each product from source system.                         | No       | '1e9e8ef04dbcff4541ed26657ea517e5' |
| category_portuguese   | VARCHAR(100)  | Product category name in Portuguese (original language).                                      | Yes      | 'beleza_saude', 'moveis_decoracao' |
| category_english      | VARCHAR(100)  | Product category name translated to English for international reporting.                      | Yes      | 'health_beauty', 'furniture_decor', 'toys' |
| name_length           | INT           | Length of the product name in characters (data quality metric).                               | Yes      | 40, 50, 60 |
| description_length    | INT           | Length of the product description in characters (indicates detail level).                     | Yes      | 150, 500, 1000 |
| photos_qty            | INT           | Number of product photos available in the listing (quality indicator).                        | Yes      | 1, 2, 3, 4 |
| weight_grams          | INT           | Product weight in grams (used for shipping calculations).                                     | Yes      | 500, 1000, 2500 |
| length_cm             | INT           | Product length in centimeters (shipping dimension).                                           | Yes      | 20, 30, 50 |
| height_cm             | INT           | Product height in centimeters (shipping dimension).                                           | Yes      | 10, 15, 25 |
| width_cm              | INT           | Product width in centimeters (shipping dimension).                                            | Yes      | 15, 20, 30 |
| volume_cm3            | INT           | Calculated product volume (length × height × width) for shipping optimization.                | Yes      | 3000, 9000, 37500 |
| create_date           | DATETIME      | Timestamp when the product record was created in the data warehouse.                          | No       | '2024-12-27 10:30:00' |

**Business Rules:**
- volume_cm3 = length_cm × height_cm × width_cm
- category_english defaults to 'Unknown' if translation not available

**Special Records:**
- product_key = -1: "UNKNOWN" product record for orders without items

---

### 3. **gold.dim_seller**
- **Purpose:** Stores information about sellers/merchants operating on the Olist marketplace.
- **Grain:** One row per unique seller_id
- **Row Count:** ~3,095 sellers
- **Source:** silver.olist_sellers_dataset
- **Columns:**

| Column Name     | Data Type     | Description                                                                                   | Nullable | Sample Values |
|----------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| seller_key     | INT           | Surrogate key uniquely identifying each seller record in the dimension table. Primary Key.    | No       | 1, 2, 3, 4... |
| seller_id      | VARCHAR(255)  | Business key - unique identifier for each seller/merchant from source system.                 | No       | '3442f8959a84dea7ee197c632cb2df15' |
| zip_code       | VARCHAR(10)   | Seller's postal code (CEP in Brazil). First 5 digits of full postal code.                    | Yes      | '13023', '01151', '04482' |
| city           | VARCHAR(100)  | Seller's city location in Brazil (fulfillment location).                                      | Yes      | 'sao paulo', 'curitiba', 'ibitinga' |
| state          | VARCHAR(2)    | Seller's state abbreviation (Brazilian states - 2 letter code).                               | Yes      | 'SP', 'PR', 'SC', 'RJ' |
| create_date    | DATETIME      | Timestamp when the seller record was created in the data warehouse.                           | No       | '2024-12-27 10:30:00' |

**Special Records:**
- seller_key = -1: "UNKNOWN" seller record for orders without items

---

### 4. **gold.dim_date**
- **Purpose:** Standard date dimension table providing calendar attributes for time-based analysis.
- **Grain:** One row per calendar date
- **Row Count:** 3,653 dates (2016-01-01 to 2025-12-31)
- **Source:** Generated table (not from source data)
- **Columns:**

| Column Name     | Data Type     | Description                                                                                   | Nullable | Sample Values |
|----------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| date_key       | INT           | Surrogate key in YYYYMMDD format (e.g., 20180115). Primary Key.                              | No       | 20160101, 20180515, 20251231 |
| full_date      | DATE          | Full calendar date in DATE format.                                                            | No       | '2018-01-15', '2017-06-20' |
| year           | INT           | Four-digit year extracted from the date.                                                      | No       | 2016, 2017, 2018 |
| quarter        | INT           | Quarter of the year (1-4).                                                                    | No       | 1, 2, 3, 4 |
| month          | INT           | Month number (1-12).                                                                          | No       | 1, 2, 3...12 |
| month_name     | VARCHAR(20)   | Full name of the month.                                                                       | No       | 'January', 'February', 'December' |
| day            | INT           | Day of the month (1-31).                                                                      | No       | 1, 15, 28, 31 |
| day_of_week    | INT           | Day of the week as number (1=Sunday, 7=Saturday in SQL Server).                               | No       | 1, 2, 3...7 |
| day_name       | VARCHAR(20)   | Full name of the day of the week.                                                             | No       | 'Monday', 'Tuesday', 'Saturday' |
| is_weekend     | BIT           | Flag indicating if the date falls on a weekend (1=Weekend, 0=Weekday).                        | No       | 0, 1 |
| is_holiday     | BIT           | Flag indicating if the date is a Brazilian public holiday (simplified list).                  | No       | 0, 1 |

**Holiday Rules (Simplified):**
- January 1: New Year's Day
- September 7: Independence Day
- December 25: Christmas

**Note:** Date dimension extends to 2025 for future planning and forecasting, though actual business data covers 2016-2018.

---

## Fact Table

### 5. **gold.fact_sales**
- **Purpose:** Central fact table storing all sales transactions with detailed metrics for orders, payments, reviews, and delivery performance.
- **Grain:** One row per order item (product sold within an order). Special rows exist for orders without items.
- **Row Count:** ~112,650 order items
- **Source:** silver.olist_orders_dataset + silver.olist_order_items_dataset + silver.olist_order_payments_dataset + silver.olist_order_reviews_dataset
- **Columns:**

#### Keys and Identifiers

| Column Name     | Data Type     | Description                                                                                   | Nullable | Sample Values |
|----------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| sales_key      | INT           | Surrogate key uniquely identifying each record in the fact table. Primary Key.                | No       | 1, 2, 3, 4... |
| order_id       | VARCHAR(255)  | Business key - unique identifier for the order from source system.                            | No       | 'e481f51cbdc54678b7cc49136f2d6af7' |
| order_item_id  | INT           | Sequential number of the item within the order (1, 2, 3...). 0 = order without items.        | No       | 0, 1, 2, 3 |

#### Foreign Keys (Dimension Links)

| Column Name     | Data Type     | Description                                                                                   | Nullable | References |
|----------------|---------------|-----------------------------------------------------------------------------------------------|----------|------------|
| customer_key   | INT           | Foreign key linking to gold.dim_customer. -1 for unknown customers.                           | No       | gold.dim_customer(customer_key) |
| product_key    | INT           | Foreign key linking to gold.dim_product. -1 for orders without items.                         | No       | gold.dim_product(product_key) |
| seller_key     | INT           | Foreign key linking to gold.dim_seller. -1 for orders without items.                          | No       | gold.dim_seller(seller_key) |
| order_date_key | INT           | Foreign key linking to gold.dim_date (YYYYMMDD format).                                       | No       | gold.dim_date(date_key) |

#### Order Status and Timestamps

| Column Name                | Data Type     | Description                                                                                   | Nullable | Sample Values |
|---------------------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| order_status              | VARCHAR(50)   | Current status of the order.                                                                  | No       | 'delivered', 'shipped', 'canceled', 'processing' |
| order_date                | DATETIME      | Timestamp when the order was placed by the customer.                                          | No       | '2017-10-02 10:56:33' |
| approval_date             | DATETIME      | Timestamp when the order payment was approved.                                                | Yes      | '2017-10-02 11:07:15' |
| shipped_date              | DATETIME      | Timestamp when the order was handed to the logistics carrier.                                 | Yes      | '2017-10-04 19:55:00' |
| delivery_date             | DATETIME      | Timestamp when the order was delivered to the customer.                                       | Yes      | '2017-10-10 21:25:13' |
| estimated_delivery_date   | DATETIME      | Estimated delivery date informed to the customer at purchase.                                 | Yes      | '2017-10-18 00:00:00' |
| shipping_limit_date       | DATETIME      | Deadline for the seller to ship the order to the carrier.                                     | Yes      | '2017-10-15 00:00:00' |

#### Financial Metrics

| Column Name          | Data Type       | Description                                                                                   | Nullable | Sample Values |
|---------------------|-----------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| item_price          | DECIMAL(10,2)   | Price of the item (original, may contain NULL).                                               | Yes      | 29.99, 158.00, 1199.90 |
| item_freight        | DECIMAL(10,2)   | Freight/shipping cost for the item (original, may contain NULL).                              | Yes      | 8.72, 19.93, 0.00 |
| item_price_clean    | DECIMAL(10,2)   | Item price with NULL replaced by 0 for calculations.                                          | No       | 29.99, 158.00, 0.00 |
| item_freight_clean  | DECIMAL(10,2)   | Item freight with NULL replaced by 0 for calculations.                                        | No       | 8.72, 19.93, 0.00 |
| total_item_value    | DECIMAL(10,2)   | Calculated total value per item (item_price_clean + item_freight_clean).                     | No       | 38.71, 177.93, 1199.90 |
| total_payment_value | DECIMAL(10,2)   | Total payment value for the entire order (aggregated from payment records).                   | No       | 141.55, 3500.00, 89.99 |

**Business Rule:** total_item_value = item_price_clean + item_freight_clean

#### Payment Metrics (Aggregated per Order)

| Column Name             | Data Type     | Description                                                                                   | Nullable | Sample Values |
|------------------------|---------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| payment_count          | INT           | Number of payment transactions for the order (some orders split payment).                     | No       | 1, 2, 3, 4 |
| primary_payment_type   | VARCHAR(50)   | Primary payment method used (based on first payment sequential).                              | Yes      | 'credit_card', 'boleto', 'voucher', 'debit_card' |
| total_installments     | INT           | Total number of installment payments across all payment methods.                              | No       | 1, 3, 6, 10, 12 |
| has_credit_card_payment| BIT           | Flag indicating if credit card was used for payment (1=Yes, 0=No).                            | No       | 0, 1 |
| has_boleto_payment     | BIT           | Flag indicating if boleto (Brazilian payment method) was used (1=Yes, 0=No).                  | No       | 0, 1 |

**Note:** Boleto is a popular Brazilian payment method (bank slip/cash payment)

#### Review Metrics (Aggregated per Order)

| Column Name        | Data Type       | Description                                                                                   | Nullable | Sample Values |
|-------------------|-----------------|-----------------------------------------------------------------------------------------------|----------|---------------|
| avg_review_score  | DECIMAL(3,2)    | Average review score for the order (1-5 scale, aggregated if multiple reviews).              | No       | 1.00, 3.00, 4.50, 5.00 |
| review_category   | VARCHAR(50)     | Categorized review score.                                                                     | Yes      | 'Excellent', 'Good', 'Average', 'Poor', 'Very Poor' |
| review_count      | INT             | Number of reviews submitted for the order.                                                    | No       | 0, 1, 2 |
| has_review_comment| BIT             | Flag indicating if the review includes a written comment (1=Yes, 0=No).                       | No       | 0, 1 |

**Review Score Categories:**
- 5: Excellent
- 4: Good  
- 3: Average
- 2: Poor
- 1: Very Poor

#### Data Quality Flags

| Column Name                | Data Type | Description                                                                                   | Nullable | Sample Values |
|---------------------------|-----------|-----------------------------------------------------------------------------------------------|----------|---------------|
| is_order_without_items    | BIT       | Flag indicating order has no items (abandoned cart or data quality issue). 1=Yes, 0=No.       | No       | 0, 1 |
| is_order_without_payment  | BIT       | Flag indicating order has no payment records. 1=Yes, 0=No.                                    | No       | 0, 1 |
| is_order_without_review   | BIT       | Flag indicating order has no customer review. 1=Yes, 0=No.                                    | No       | 0, 1 |

#### Delivery Performance Metrics

| Column Name               | Data Type | Description                                                                                   | Nullable | Sample Values |
|--------------------------|-----------|-----------------------------------------------------------------------------------------------|----------|---------------|
| days_to_approval         | INT       | Number of days from order placement to payment approval.                                      | Yes      | 0, 1, 2, 3 |
| days_to_shipping         | INT       | Number of days from order placement to carrier handoff.                                       | Yes      | 1, 2, 3, 5 |
| days_to_delivery         | INT       | Number of days from order placement to customer delivery.                                     | Yes      | 5, 7, 10, 15, 20 |
| delivery_vs_estimate_days| INT       | Difference between actual and estimated delivery (negative=early, positive=late).             | Yes      | -5, -2, 0, 3, 10 |
| is_late_delivery         | BIT       | Flag indicating if delivery was after estimated date or still pending past estimate. 1=Late.  | No       | 0, 1 |

**Calculation Rules:**
- days_to_approval = DATEDIFF(DAY, order_date, approval_date)
- days_to_delivery = DATEDIFF(DAY, order_date, delivery_date)
- delivery_vs_estimate_days = DATEDIFF(DAY, estimated_delivery_date, delivery_date)
- Negative delivery_vs_estimate_days = Early delivery
- Positive delivery_vs_estimate_days = Late delivery

#### Order Status Flags

| Column Name   | Data Type | Description                                                                                   | Nullable | Sample Values |
|--------------|-----------|-----------------------------------------------------------------------------------------------|----------|---------------|
| is_delivered | BIT       | Flag indicating if order status is 'delivered'. 1=Yes, 0=No.                                  | No       | 0, 1 |
| is_canceled  | BIT       | Flag indicating if order status is 'canceled'. 1=Yes, 0=No.                                   | No       | 0, 1 |
| is_shipped   | BIT       | Flag indicating if order has been shipped or delivered. 1=Yes, 0=No.                          | No       | 0, 1 |

---

## Data Lineage

### Bronze → Silver → Gold Flow

```
BRONZE LAYER (Raw Data)
├── bronze.olist_customers_dataset
├── bronze.olist_orders_dataset
├── bronze.olist_order_items_dataset
├── bronze.olist_products_dataset
├── bronze.olist_sellers_dataset
├── bronze.olist_order_payments_dataset
├── bronze.olist_order_reviews_dataset
├── bronze.olist_geolocation_dataset
└── bronze.product_category_name_translation

        ↓ (Data Cleaning, Validation, Type Conversion)

SILVER LAYER (Cleaned Data)
├── silver.olist_customers_dataset
├── silver.olist_orders_dataset
├── silver.olist_order_items_dataset
├── silver.olist_products_dataset
├── silver.olist_sellers_dataset
├── silver.olist_order_payments_dataset
├── silver.olist_order_reviews_dataset
├── silver.olist_geolocation_dataset
└── silver.product_category_name_translation

        ↓ (Business Logic, Enrichment, Star Schema)

GOLD LAYER (Business Ready)
├── gold.dim_customer (from silver.olist_customers_dataset + silver.olist_geolocation_dataset)
├── gold.dim_product (from silver.olist_products_dataset + silver.product_category_name_translation)
├── gold.dim_seller (from silver.olist_sellers_dataset)
├── gold.dim_date (generated)
└── gold.fact_sales (from all silver tables)
```

---

## Relationships

### Star Schema Relationships

| From Table (Dimension) | PK Column      | To Table (Fact)  | FK Column        | Cardinality | Description |
|-----------------------|----------------|------------------|------------------|-------------|-------------|
| gold.dim_customer     | customer_key   | gold.fact_sales  | customer_key     | 1:M         | One customer can place many orders |
| gold.dim_product      | product_key    | gold.fact_sales  | product_key      | 1:M         | One product can be sold many times |
| gold.dim_seller       | seller_key     | gold.fact_sales  | seller_key       | 1:M         | One seller can sell many items |
| gold.dim_date         | date_key       | gold.fact_sales  | order_date_key   | 1:M         | One date can have many orders |

---

## Business Glossary

### Key Terms

**Customer Unique ID vs Customer ID:**
- `customer_id`: Unique per order (changes with each order)
- `customer_unique_id`: Unique per person (same across all orders from one customer)
- Use `customer_unique_id` for customer retention and lifetime value analysis

**Payment Types:**
- `credit_card`: Credit card payment
- `boleto`: Brazilian bank slip (cash payment at bank/lottery/convenience store)
- `voucher`: Gift card or promotional voucher
- `debit_card`: Debit card payment

**Order Status Values:**
- `delivered`: Order successfully delivered to customer
- `shipped`: Order handed to carrier but not yet delivered
- `canceled`: Order canceled by customer or system
- `processing`: Order being prepared by seller
- `invoiced`: Payment processed, awaiting shipment
- `unavailable`: Product unavailable, order cannot be fulfilled

**Review Score Scale:**
- 5 = Excellent (Very satisfied)
- 4 = Good (Satisfied)
- 3 = Average (Neutral)
- 2 = Poor (Dissatisfied)
- 1 = Very Poor (Very dissatisfied)

**Brazilian State Codes:**
- SP = São Paulo
- RJ = Rio de Janeiro
- MG = Minas Gerais
- RS = Rio Grande do Sul
- PR = Paraná
- SC = Santa Catarina
- (and 21 other states)

---

## Data Quality Notes

### Known Issues and Handling

1. **Orders Without Items (is_order_without_items = 1):**
   - Approximately 610 orders have no items
   - Likely abandoned carts or data collection issues
   - These orders have product_key = -1 and seller_key = -1

2. **Orders Without Payment (is_order_without_payment = 1):**
   - Some orders have no payment records
   - May indicate test orders or data quality issues

3. **Orders Without Reviews (is_order_without_review = 1):**
   - Many orders have no customer review
   - Not all customers leave reviews (normal behavior)

4. **Missing Geolocation:**
   - Some customer/seller zip codes don't have latitude/longitude
   - Results in NULL values for geographic coordinates

5. **Special Dimension Records:**
   - All dimensions include a -1 key for "UNKNOWN" or missing references
   - Ensures referential integrity (no orphaned records in fact table)

---

## Usage Examples

### Common Analytical Queries

**Total Revenue by Year:**
```sql
SELECT 
    d.year,
    SUM(f.total_item_value) AS total_revenue
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.order_date_key = d.date_key
WHERE f.is_order_without_items = 0
GROUP BY d.year
ORDER BY d.year;
```

**Customer Retention (Repeat Customers):**
```sql
SELECT 
    customer_unique_id,
    COUNT(DISTINCT order_id) AS order_count
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY customer_unique_id
HAVING COUNT(DISTINCT order_id) > 1;
```

**Delivery Performance by State:**
```sql
SELECT 
    c.state,
    AVG(f.days_to_delivery) AS avg_delivery_days,
    SUM(CAST(f.is_late_delivery AS INT)) * 100.0 / COUNT(*) AS late_delivery_pct
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
WHERE f.is_delivered = 1
GROUP BY c.state;
```

**Top Product Categories:**
```sql
SELECT 
    p.category_english,
    COUNT(*) AS items_sold,
    SUM(f.total_item_value) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE f.product_key != -1
GROUP BY p.category_english
ORDER BY revenue DESC;
```

---

## Metadata

**Last Updated:** 2024-12-27  
**Data Warehouse Version:** 1.0  
**Contact:** Data Engineering Team  
**Documentation Status:** Complete

**Change Log:**
- 2024-12-27: Initial Gold Layer documentation created
- 2024-12-27: Added comprehensive column descriptions
- 2024-12-27: Added business glossary and usage examples
