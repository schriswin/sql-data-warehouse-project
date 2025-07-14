/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV files to Bronze layer.

Parameters:
    None.
  This Stored Procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
=============================================================
*/

-- Full loading --

 CREATE OR ALTER PROCEDURE bronze.load_bronze AS
 BEGIN
	BEGIN TRY

		 DECLARE @start_time DATETIME, @end_time DATETIME;

		 PRINT '========================================';
		 PRINT 'LOADING BRONZE LAYER';
		 PRINT '========================================';

		 PRINT '----------------------------------------';
		 PRINT 'LOADING CRM TABLES';
		 PRINT '----------------------------------------';

		 SET @start_time = GETDATE();
		 PRINT '>> Truncate Table: bronze.crm_cust_info'
		 TRUNCATE TABLE bronze.crm_cust_info

		 PRINT '>> Inserting data into: bronze.crm_cust_info'
		 BULK INSERT bronze.crm_cust_info
		 FROM 'D:\SCRIS\DWH project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		 WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 )
		 SET @end_time = GETDATE();
		 PRINT 'Time to create crm_cust_info: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		 PRINT '----------------------------------------';
		 

		 SET @start_time = GETDATE();
		 PRINT '>> Truncate Table: bronze.crm_prd_info'
		 TRUNCATE TABLE bronze.crm_prd_info

		 PRINT '>> Inserting data into: bronze.crm_prd_info'
		 BULK INSERT bronze.crm_prd_info
		 FROM 'D:\SCRIS\DWH project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		 WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 )
		 SET @end_time = GETDATE()
		 PRINT 'Time to create crm_prd_info: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		 PRINT '----------------------------------------';


		 SET @start_time = GETDATE()
		 PRINT '>> Truncate Table: bronze.crm_sales_details'
		 TRUNCATE TABLE bronze.crm_sales_details

		 PRINT '>> Inserting data into: bronze.crm_sales_details'
		 BULK INSERT bronze.crm_sales_details
		 FROM 'D:\SCRIS\DWH project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		 WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 )
		 SET @end_time = GETDATE()
		 PRINT 'Time to create crm_sales_details: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		 PRINT '----------------------------------------';

		 PRINT '----------------------------------------';
		 PRINT 'LOADING ERP TABLES';
		 PRINT '----------------------------------------';

		 SET @start_time = GETDATE()
		 PRINT '>> Truncate Table: bronze.erp_cust_az12'
		 TRUNCATE TABLE bronze.erp_cust_az12

		 PRINT '>> Inserting data into: bronze.erp_cust_az12'
		 BULK INSERT bronze.erp_cust_az12
		 FROM 'D:\SCRIS\DWH project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		 WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 )
		 SET @end_time = GETDATE()
		 PRINT 'Time to create erp_cust_az12: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		 PRINT '----------------------------------------';

			
		 SET @start_time = GETDATE()
		 PRINT '>> Truncate Table: bronze.erp_loc_a101'
		 TRUNCATE TABLE bronze.erp_loc_a101

		 PRINT '>> Inserting data into: bronze.erp_loc_a101'
		 BULK INSERT bronze.erp_loc_a101
		 FROM 'D:\SCRIS\DWH project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		 WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 )
		 SET @end_time = GETDATE()
		 PRINT 'Time to create erp_loc_a101: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		 PRINT '----------------------------------------';


		 SET @start_time = GETDATE()
		 PRINT '>> Truncate Table: bronze.erp_px_cat_g1v2'
		 TRUNCATE TABLE bronze.erp_px_cat_g1v2

		 PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2'
		 BULK INSERT bronze.erp_px_cat_g1v2
		 FROM 'D:\SCRIS\DWH project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		 WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		 )
		 SET @end_time = GETDATE()
		 PRINT 'Time to create erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		 PRINT '----------------------------------------';

	END TRY

	BEGIN CATCH
		PRINT '=========================';
		PRINT 'Error: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=========================';
	END CATCH
END
