/*
=============================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
=============================================================
Script purpose:
    This stored procedure performs the ETL (Extract, Load, Transform, Load) process to populate
    'silver' schema tables from the 'bronze' schema.
Actions performed:
    - Truncates the silver tables before loading data.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
  This Stored Procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
=============================================================
*/

-- Do Transformations and load to Silver layer  --

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		
		DECLARE @start_time DATETIME, @end_time DATETIME;

		PRINT '========================================';
		PRINT 'LOADING SILVER LAYER';
		PRINT '========================================';

		PRINT '----------------------------------------';
		PRINT 'LOADING CRM TABLES';
	    PRINT '----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.crm_cust_info'

		PRINT '>> Truncating table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info 
		(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN cst_marital_status = 'S' THEN 'Single'
				WHEN cst_marital_status = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT *,
				   ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS cst_id_rank
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS sub
		WHERE cst_id_rank = 1;

		SET @end_time = GETDATE()
		PRINT 'Time to create crm_cust_info: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.crm_prd_info'

		PRINT '>> Truncating table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		SELECT prd_id,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
				SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost,0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(
					DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))
					AS DATE
				) AS prd_end_dt
		FROM bronze.crm_prd_info

		SET @end_time = GETDATE()
		PRINT 'Time to create crm_prd_info: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.crm_sales_details'

		PRINT '>> Truncating table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		SELECT sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE
					WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE
					WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE
					WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE
					WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity * ABS(sls_price)
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,
				sls_quantity,
				CASE 
					WHEN sls_price IS NULL or sls_price<=0
						THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
				END AS sls_price

		FROM bronze.crm_sales_details

		SET @end_time = GETDATE()
		PRINT 'Time to create crm_sales_details: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------';

		PRINT '----------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.erp_cust_az12'

		PRINT '>> Truncating table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		SELECT	CASE 
					WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid
				END AS cid,
				CASE 
					WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END AS bdate, 
				CASE 
					WHEN UPPER(TRIM(gen)) IN ('Male','M') THEN 'Male'
					WHEN UPPER(TRIM(gen)) IN ('Female','F') THEN 'Female'
					ELSE 'n/a'
				END AS gen

		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE()
		PRINT 'Time to create erp_cust_az12: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.erp_loc_a101'

		PRINT '>> Truncating table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT REPLACE(cid,'-','') AS cid,
				CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
					 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					 ELSE TRIM(cntry)
				END AS cntry

		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE()
		PRINT 'Time to create erp_loc_a101: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncate Table: silver.erp_px_cat_g1v2'

		PRINT '>> Truncating table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT id,
				cat,
				subcat,
				maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE()
		PRINT 'Time to create erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec'
		PRINT '----------------------------------------';
	END TRY

	BEGIN CATCH
		PRINT '=========================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=========================';		
	END CATCH
END
