/*
===============================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy
    and standardizations across the 'silver' schema. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
-- Silver - Data Correctness Checks --

-- silver.crm_cust_info --
SELECT * FROM silver.crm_cust_info
-- Ensure each primary key has 1 record only --
SELECT cst_id,
		COUNT(cst_id)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1

-- Check for unwanted spaces --
-- Expectation: No result --
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
-- Data standardization and consistency --
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


-- silver.crm_prd_info --
SELECT * FROM bronze.crm_prd_info
-- Ensure each primary key has 1 record only --
SELECT prd_id,
		COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
-- Check for unwanted spaces --
-- Expectation: No result --
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
-- Check for NULLs or Negative numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0
-- Data standardization and consistency --
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
-- Check for invalid date orders --
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt


-- silver.crm_sales_details --
SELECT * FROM bronze.crm_sales_details
-- Check for unwanted spaces --
-- Expectation: No result --
SELECT sls_ord_num FROM bronze.crm_sales_details
WHERE TRIM(sls_ord_num) != sls_ord_num
-- Check for invalid date orders --
SELECT NULLIF(sls_due_dt,0) as sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt)!=8 
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101
-- Check for invalid date orders --
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_due_dt or sls_order_dt > sls_ship_dt
-- Check for invalid business logics --
SELECT sls_sales,sls_quantity,sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price


-- silver.erp_cust_az12 --
SELECT cid
FROM silver.erp_cust_az12
WHERE TRIM(cid)!=cid
-- Check Out-of-range dates --
SELECT bdate FROM silver.erp_cust_az12
WHERE bdate < '1924-02-10' and bdate > GETDATE()
-- Data Standardization & Consistency -- 
SELECT DISTINCT gen FROM silver.erp_cust_az12


-- silver.erp_loc_a101 --
SELECT cid FROM silver.erp_loc_a101
WHERE cid NOT LIKE 'AW%'
-- Data standardization and consistency --
SELECT DISTINCT cntry FROM silver.erp_loc_a101
ORDER BY cntry


-- silver.erp_px_cat_g1v2 --
-- Check for unwanted Spaces --
SELECT * FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat!=TRIM(subcat) OR maintenance != TRIM(maintenance)
-- Data standardization & consistency --
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2
