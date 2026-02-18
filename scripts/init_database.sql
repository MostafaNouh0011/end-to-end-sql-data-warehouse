/*
=============================================================
  Project: Modern Data Warehouse (Medallion Architecture)
  Database: DataWarehouse
  Platform: Microsoft SQL Server

  ⚠ WARNING:
  This script will DROP and recreate the 'DataWarehouse' database if it exists.
  Running this script will permanently delete all existing data inside the database.

  Use this script ONLY in development or testing environments.
  Do NOT run in production.

  Description:
  This script creates a new database called 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, defines three schemas representing the Medallion.
  
  Architecture layers:
    1. bronze  → Raw data layer (source ingestion)
    2. silver  → Cleaned and transformed layer
    3. gold    → Business-ready analytics layer
=============================================================
*/

USE master;
GO

-- Drop database if it exists
IF DB_ID('DataWarehouse') IS NOT NULL
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch to the new database
USE DataWarehouse;
GO

-- Create schemas (layers)
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
