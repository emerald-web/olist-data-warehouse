# **Olist Data Warehouse and Analytics Project**

Welcome to the **Olist Data Warehouse and Analytics Project** repository.
This portfolio project demonstrates a complete data warehousing and analytics solution using SQL Server — from data extraction and transformation to modeling and reporting.
It follows industry best practices in **data engineering**, **data modeling**, and **analytics**.

---

## **🏗️ Data Architecture**

The project implements the **Medallion Architecture**, structured into three core layers — **Bronze**, **Silver**, and **Gold**.

<img width="1026" height="623" alt="Olist Warehouse drawio" src="https://github.com/user-attachments/assets/3f5bd27a-621f-4d79-b04c-b5859968d66b" />


1. **Bronze Layer**

   * Stores raw Olist data extracted from CSV files.
   * Represents data “as-is” from the source systems (ERP & CRM equivalents).

2. **Silver Layer**

   * Applies data cleansing, standardization, and conformance.
   * Ensures consistent formats and removes duplicates or invalid records.
   * All transformations occur here except business logic, aggregation, and data integration.

3. **Gold Layer**

   * Contains business-ready, analytics-optimized data.
   * Applies **business rules**, **aggregations**, and **data integration**.
   * Modeled in a **Star Schema** with fact and dimension tables.

---

## **📖 Project Overview**

This project demonstrates the end-to-end process of building a modern data warehouse using the **Olist E-Commerce dataset**.

### Key Components

1. **Data Architecture** – Designed using the Medallion pattern (Bronze → Silver → Gold).
2. **ETL Pipelines** – Implemented with full extraction, batch load, and file parsing.
3. **Data Modeling** – Gold layer modeled into star schema with dimension and fact tables.
4. **Analytics & Reporting** – SQL-based analysis focusing on customer behavior, product performance, and sales trends.

### Skills Demonstrated

* SQL Development
* Data Architecture
* Data Engineering
* ETL Pipeline Design
* Data Modeling
* Business Data Analytics

---

## **🧰 Tools and Resources**

* **Dataset**: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
* **SQL Server Express** – For hosting the Data Warehouse.
* **SQL Server Management Studio (SSMS)** – For database management and development.
* **DrawIO** – For data flow and architecture diagrams.
* **Notion** – For project documentation and planning.
* **GitHub** – For version control and portfolio presentation.

---

## **🚀 Project Requirements**

### **Data Warehouse (Data Engineering)**

#### Objective

To build a modern data warehouse consolidating Olist sales and customer data, enabling analytical reporting and insight generation.

#### Specifications

* **Data Sources**: Multiple CSVs representing e-commerce systems (orders, payments, products, customers, etc.).
* **Data Quality**: Cleanse and resolve missing or inconsistent data before analysis.
* **Integration**: Combine all datasets into a unified analytical model.
* **Scope**: Focus on the existing dataset (no historization).
* **Documentation**: Provide model diagrams, data catalogs, and transformation logic.

---

### **Analytics & Reporting (Data Analysis)**

#### Objective

Develop SQL analytics to deliver insights on:

* Customer purchasing behavior.
* Product performance and profitability.
* Sales and revenue trends across time and regions.

These insights empower data-driven decision-making for business growth.
Detailed requirements are documented in [docs/requirements.md](docs/requirements.md).

---

## **📂 Repository Structure**

```
olist-data-warehouse/
│
├── datasets/                           # Olist CSV datasets (orders, products, customers, etc.)
│
├── docs/                               # Documentation and architecture files
│   ├── etl.drawio                      # ETL process diagram
│   ├── data_architecture.drawio        # Medallion architecture design
│   ├── data_catalog.md                 # Dataset metadata and field descriptions
│   ├── data_flow.drawio                # Data flow from ingestion to warehouse
│   ├── data_models.drawio              # Star schema diagrams (fact/dim tables)
│   ├── naming-conventions.md           # Object naming standards
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Raw data ingestion scripts
│   ├── silver/                         # Data cleansing and transformation scripts
│   ├── gold/                           # Star schema and business logic scripts
│
├── tests/                              # Validation and quality assurance scripts
│
├── README.md                           # Project overview and documentation
├── LICENSE                             # License details
├── .gitignore                          # Git ignored files
└── requirements.txt                    # Dependencies and tools
```

---

## **🛡️ License**

This project is licensed under the **MIT License**.
You may use, modify, and share this repository with proper attribution.

---

## **👤 About Me**

I am **Okenwa Emmanuel Ikechukwu**, a Data Analyst and Data Engineer focused on building scalable data systems and delivering insights that support business growth.
My work combines SQL development, data modeling, ETL engineering, data warehousing, and analytics to create efficient, reliable, and well-documented data solutions. 
I am driven by the goal of transforming data into a strategic asset — improving decision-making and organizational performance through sound engineering and analysis.

**Connect with me on:**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge\&logo=linkedin\&logoColor=white)](https://www.linkedin.com/in/emmanuel-okenwa/)


