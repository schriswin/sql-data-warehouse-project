/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the databse exists, it is dropped and recreated. Additionally, the script sets up 3 schemas within the database: 'bronze', 'silver' and 'gold'.

Warning: 
    Running the script will drop the entire 'DataWarehouse' database if it exists.
    All data in the databse will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before running this script.
*/


USE master;
GO

-- Drop and recreate the 'DateWarehouse' database
IF EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = 'DateWarehouse'
)
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create SchemaS
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
