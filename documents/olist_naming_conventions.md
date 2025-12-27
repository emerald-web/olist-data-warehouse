
# 🏗️ Olist Data Warehouse Naming Convention

## **1️⃣ General Principles**
- Use **snake_case** for all object names (lowercase + underscores).
- Always use **English** for naming.
- Avoid using **SQL reserved words**.
- Use **clear, self-descriptive names** that reveal purpose and content.
- Maintain **consistency** across layers (Bronze, Silver, Gold).

---

## **2️⃣ Table Naming Convention**

### **🟤 Bronze Layer (Raw Zone)**  
Stores raw data as ingested from source systems (no transformations).

**Pattern:**  
`<source_system>_<entity>`  

- `<source_system>` → name of data origin (e.g., `olist`, `geolocation`, `marketing`)  
- `<entity>` → original table name  

**Examples:**  
- `olist_orders`  
- `olist_customers`  
- `olist_order_items`  
- `olist_products`  

✅ *Purpose:* Maintain 1:1 structure with source tables for traceability.  

---

### **⚪ Silver Layer (Cleansed Zone)**  
Contains cleansed, validated, and standardized data.  

**Pattern:**  
`slv_<entity>`  

- Prefix `slv_` stands for **Silver** layer  
- `<entity>` → descriptive business term  

**Examples:**  
- `slv_orders`  
- `slv_customers`  
- `slv_products`  

✅ *Purpose:* Standardized schema ready for modeling.  

---

### **🟡 Gold Layer (Business Zone)**  
Contains final business-ready models (facts, dimensions, reports).  

**Pattern:**  
`<category>_<entity>`  

- `<category>`:  
  - `dim_` → Dimension table  
  - `fact_` → Fact table  
  - `report_` → Prebuilt report tables  

**Examples:**  
- `dim_customer`  
- `dim_product`  
- `fact_sales`  
- `report_sales_summary`  

✅ *Purpose:* Used for analytics, dashboards, and KPIs.

---

## **3️⃣ Column Naming Convention**

### **🗝️ Surrogate Keys**
- All dimension table keys end with `_key`.  
**Pattern:** `<entity>_key`

**Examples:**  
- `customer_key`  
- `product_key`  

---

### **⚙️ Technical Columns**
Used for ETL metadata and warehouse operations.

**Pattern:** `dwh_<description>`  

**Examples:**  
- `dwh_load_date` → Date when record was loaded  
- `dwh_updated_by` → ETL process or user who updated record  

---

## **4️⃣ Stored Procedure Convention**

Used for data loading across layers.  

**Pattern:**  
`load_<layer>`  

**Examples:**  
- `load_bronze` → Extract and ingest raw data  
- `load_silver` → Clean and standardize data  
- `load_gold` → Build analytical models  

---

## **5️⃣ Example Summary Table**

| Layer   | Pattern Example         | Description                        |
|----------|-------------------------|------------------------------------|
| Bronze  | `olist_orders`          | Raw data from Olist source system |
| Silver  | `slv_orders`            | Cleaned and standardized data     |
| Gold    | `fact_sales`, `dim_customer` | Analytical and reporting tables   |
