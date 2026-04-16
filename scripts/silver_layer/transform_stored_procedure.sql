CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	
	DECLARE @start_time AS DATETIME, @end_time AS DATETIME, @load_start_time AS DATETIME, @load_end_time AS DATETIME;
	
	BEGIN TRY
		PRINT '============================================================================================================================';
		PRINT 'Loading Silver Layer ...'
		PRINT '============================================================================================================================';

		PRINT '============================================================================================================================';
		PRINT 'Loading CRM Tables ...'
		PRINT '============================================================================================================================';

		SET @load_start_time = GETDATE();
		SET @start_time = GETDATE();

		PRINT '>>Truncating and loading silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM(
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) AS t WHERE flag_last = 1

		SET @end_time = GETDATE();

		PRINT 'Time taken to load silver.crm_cust_info table : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '============================================================================================================================';

		SET @start_time = GETDATE();

		PRINT '>>Truncating and loading silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		DATEADD(day,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM(
			SELECT
			*
			FROM bronze.crm_prd_info
		) AS t;

		SET @end_time = GETDATE();

		PRINT 'Time taken to load silver.crm_prd_info table : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '============================================================================================================================';

		SET @start_time = GETDATE();

		PRINT '>>Truncating and loading silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) < 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) 
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) < 8 THEN NULL
			WHEN sls_order_dt > sls_ship_dt THEN DATEADD(day, -1, CAST(CAST(LEAD(sls_order_dt) OVER (PARTITION BY sls_ord_num ORDER BY sls_order_dt) AS NVARCHAR) AS DATE))
			ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) 
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) < 8 THEN NULL
			WHEN sls_order_dt > sls_due_dt THEN DATEADD(day, -1, CAST(CAST(LEAD(sls_order_dt) OVER (PARTITION BY sls_ord_num ORDER BY sls_order_dt) AS NVARCHAR) AS DATE))
			ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN (sls_sales != sls_quantity * sls_price) OR (sls_sales IS NULL) OR (sls_sales < 0) THEN ABS(sls_quantity) * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		CASE 
			WHEN (sls_quantity != sls_sales / sls_price) OR (sls_quantity IS NULL) OR (sls_quantity < 0) THEN ABS(sls_sales) / ABS(sls_price)
			ELSE sls_quantity
		END AS sls_quantity,
		CASE
			WHEN (sls_price != sls_sales / sls_quantity) OR (sls_price IS NULL) OR (sls_price < 0) THEN ABS(sls_sales) / ABS(sls_quantity)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();

		PRINT 'Time taken to load silver.crm_sales_details table : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '============================================================================================================================';
		PRINT 'Loading ERP Tables ...';
		PRINT '============================================================================================================================';

		SET @start_time = GETDATE();

		PRINT '>>Truncating and loading silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12
		(
			CID,
			BDATE,
			GEN
		)
		SELECT
		CASE
			WHEN CID LIKE 'NAS%' THEN TRIM(SUBSTRING(CID, 4, LEN(CID)))
			ELSE TRIM(CID)
		END AS CID,
		CASE
			WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE,
		CASE
			WHEN GEN IS NULL OR GEN = ' ' THEN 'n/a'
			WHEN TRIM(UPPER(GEN)) IN ('M', 'MALE') THEN 'Male'
			WHEN TRIM(UPPER(GEN)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE TRIM(GEN)
		END AS GEN
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();

		PRINT 'Time taken to load silver.erp_cust_az12 table : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '============================================================================================================================';

		SET @start_time = GETDATE();

		PRINT '>>Truncating and loading silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101
		(
			CID,
			CNTRY
		)
		SELECT
		CASE
			WHEN CID LIKE '%-%' THEN REPLACE(UPPER(TRIM(CID)), '-', '')
			ELSE CID
		END AS CID,
		CASE
			WHEN (CNTRY IS NULL) OR (TRIM(CNTRY) = '') THEN 'n/a'
			WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'US') THEN 'United States'
			WHEN UPPER(TRIM(CNTRY)) = 'UK' THEN 'United Kingdom'
			WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
			ELSE TRIM(CNTRY)
		END AS CNTRY
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();

		PRINT 'Time taken to load silver.erp_loc_a101 table : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '============================================================================================================================';

		SET @start_time = GETDATE();

		PRINT '>>Truncating and loading silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_px_cat_g1v2
		(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT
		ID,
		CAT,
		REPLACE(SUBCAT, '-', ' ') AS SUBCAT,
		MAINTENANCE
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		SET @load_end_time = GETDATE();

		PRINT 'Time taken to silver.erp_px_cat_g1v2 table : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '***Time taken to load silver layer : ' + CAST(DATEDIFF(second, @load_start_time, @load_end_time) AS NVARCHAR) + ' seconds***';

		PRINT '============================================================================================================================';
	END TRY
	BEGIN CATCH
		PRINT 'Some error occured while loading silver layer';
		PRINT 'Error message : ' + ERROR_MESSAGE();
		PRINT 'Error number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error state : ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END

