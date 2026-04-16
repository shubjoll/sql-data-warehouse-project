CREATE OR ALTER PROCEDURE bronze.load_bronze AS

	BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME;
		DECLARE @load_time_start DATETIME, @load_time_end DATETIME;
		BEGIN TRY
			
			PRINT '=======================================================================';
			PRINT 'Loading Bronze layer';
			PRINT '=======================================================================';

			PRINT '-----------------------------------------------------------------------';
			PRINT '>> Truncating and loading table : crm_cust_info';
			PRINT '-----------------------------------------------------------------------';
			SET @load_time_start = GETDATE();
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_cust_info;
			BULK INSERT bronze.crm_cust_info
			FROM 'D:\Data practice\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Time taken to load table crm_cust_info : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';

			PRINT '-----------------------------------------------------------------------';
			PRINT '>> Truncating and loading table : crm_prd_info';
			PRINT '-----------------------------------------------------------------------';
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_prd_info;
			BULK INSERT bronze.crm_prd_info
			FROM 'D:\Data practice\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Time taken to load table crm_prd_info : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';

			PRINT '-----------------------------------------------------------------------';
			PRINT '>> Truncating and loading table : crm_sales_details';
			PRINT '-----------------------------------------------------------------------';
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.crm_sales_details
			BULK INSERT bronze.crm_sales_details
			FROM 'D:\Data practice\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Time taken to load table crm_sales_details : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';

			PRINT '-----------------------------------------------------------------------';
			PRINT '>> Truncating and loading table : erp_cust_az12';
			PRINT '-----------------------------------------------------------------------';
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_cust_az12
			BULK INSERT bronze.erp_cust_az12
			FROM 'D:\Data practice\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Time taken to load table erp_cust_az12 : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';

			PRINT '-----------------------------------------------------------------------';
			PRINT '>> Truncating and loading table : erp_loc_a101';
			PRINT '-----------------------------------------------------------------------';
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_loc_a101
			BULK INSERT bronze.erp_loc_a101
			FROM 'D:\Data practice\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Time taken to load table erp_loc_a101 : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';

			PRINT '-----------------------------------------------------------------------';
			PRINT '>> Truncating and loading table : erp_px_cat_g1v2';
			PRINT '-----------------------------------------------------------------------';
			SET @start_time = GETDATE();
			TRUNCATE TABLE bronze.erp_px_cat_g1v2
			BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'D:\Data practice\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
			SET @end_time = GETDATE();
			PRINT '>> Time taken to load table erp_px_cat_g1v2 : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds <<';
			SET @load_time_end = GETDATE();

			PRINT '*** Time taken to load bronze layer : '+ CAST(DATEDIFF(second, @load_time_start, @load_time_end) AS NVARCHAR) + ' seconds ***';
			END TRY
			BEGIN CATCH
				PRINT 'Error occured while loading Bronze layer';
				PRINT '>> Error message:'+ ERROR_MESSAGE();
				PRINT '>> Error number: '+ CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT '>> Error state: '+ CAST(ERROR_STATE() AS NVARCHAR);
			END CATCH

	END

