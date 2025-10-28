/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop the database if it already exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    DROP DATABASE DataWarehouse;
END
GO  

USE master;
GO

-- Create a new database for the data warehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create schemas for different data layers
CREATE SCHEMA bronze;  
GO

CREATE SCHEMA silver;  
GO

CREATE SCHEMA gold; 
GO
