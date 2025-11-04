/*
=========================================================================================
                           OLIST DATA WAREHOUSE INITIALIZATION SCRIPT
=========================================================================================

📌 Purpose:
    This script initializes the Olist Data Warehouse environment. 
    It creates the core database and schema structure following the 
    Medallion Architecture — Bronze, Silver, and Gold layers.

🎯 Key Features:
    - Checks if the database already exists and drops it (for rebuilds)
    - Creates the Olist_DataWarehouse database
    - Creates core schemas: bronze, silver, gold

⚠️ WARNING:
    Running this script will DROP the existing database named 'Olist_DataWarehouse'
    if it already exists — all existing data, tables, and objects will be permanently deleted.
    Use this script only in a development or testing environment.

📂 Author: Okenwa Emmanuel Ikechukwu
📅 Date: 2025-11-04
=========================================================================================
*/

-- ==========================================
-- 1️⃣ Drop Database if it Already Exists
-- ==========================================
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'Olist_DataWarehouse')
BEGIN
    PRINT 'Database Olist_DataWarehouse already exists. Dropping existing database...';
    ALTER DATABASE Olist_DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Olist_DataWarehouse;
    PRINT 'Database dropped successfully.';
END
GO

-- ==========================================
-- 2️⃣ Create New Database
-- ==========================================
PRINT 'Creating new database: Olist_DataWarehouse...';
CREATE DATABASE Olist_DataWarehouse;
GO

USE Olist_DataWarehouse;
GO

PRINT 'Database context switched to Olist_DataWarehouse.';
GO

-- ==========================================
-- 3️⃣ Create Schemas (Bronze, Silver, Gold)
-- ==========================================
PRINT 'Creating Medallion Architecture Schemas...';

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

PRINT 'Schemas created successfully.';
GO

PRINT '✅ Olist Data Warehouse initialization completed successfully.';
GO
