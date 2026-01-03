/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
create or alter procedure bronze.load_bronze as
BEGIN
	DECLARE @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
	BEGIN TRY
	set @batch_start_time=GETDATE();
	print'=========================================';
	print'Loading bronze layer';
	print'========================================='
	;
	
	print'-----------------------------------------';
	set @start_time=GETDATE();
	print'>> Truncating bronze.crm_cust_info Table ';
	truncate table bronze.crm_cust_info;
	print'>> Inserting bronze.crm_cust_info Table ';
	bulk insert bronze.crm_cust_info
	from 'C:\Users\mailp\Downloads\cust_info.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time = GETDATE();
	print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';
	
	print'--------------------------------------------';
	set @start_time=GETDATE();
	print'>> truncating table [bronze].[crm_prd_info] ';
	truncate table [bronze].[crm_prd_info];
	print'>> Inserting table [bronze].[crm_prd_info]';
	bulk insert [bronze].[crm_prd_info]
	from 'C:\Users\mailp\Downloads\prd_info.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=GETDATE();
	print'Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';
	

	print'-----------------------------------------------';
	print'>>Truncating table [bronze].[crm_sales_details]';
	set @start_time=GETDATE();
	truncate table [bronze].[crm_sales_details];
	print'>>Inserting table [bronze].[crm_sales_details]';
	bulk insert [bronze].[crm_sales_details]
	from 'C:\Users\mailp\Downloads\sales_details.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=GETDATE();
	print'Loading Duration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';

	
	print'-------------------------------------------------';
	print'>>Truncating table [bronze].[erp_cust_az12]';
	set @start_time=GETDATE();
	truncate table [bronze].[erp_cust_az12];
	print'>>Inserting table [bronze].[erp_cust_az12]';
	bulk insert [bronze].[erp_cust_az12]
	from 'C:\Users\mailp\Downloads\CUST_AZ12.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=GETDATE();
	print'Loading Duration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';
	
	print'--------------------------------------------------';
	print'>>Truncating [bronze].[erp_loc_a101]';
	set @start_time=GETDATE();
	truncate table [bronze].[erp_loc_a101];
	print'>>Inserting table [bronze].[erp_loc_a101]';
	bulk insert [bronze].[erp_loc_a101]
	from 'C:\Users\mailp\Downloads\LOC_A101.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=GETDATE();
	print'Loading Duration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';
	
	print'----------------------------------------------------';
	print'>>Truncating table [bronze].[erp_px_cat_g1v2]';
	set @start_time=GETDATE();
	truncate table [bronze].[erp_px_cat_g1v2];
	print'>>Inserting table [bronze].[erp_px_cat_g1v2]';
	bulk insert [bronze].[erp_px_cat_g1v2]
	from 'C:\Users\mailp\Downloads\PX_CAT_G1V2.csv'
	with(
	firstrow=2,
	fieldterminator=',',
	tablock
	);
	set @end_time=GETDATE();
	print'Loading Duration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';

	print'-----------------------------------------------------------';
	print'Loading bronze layer is completed';
	set @batch_end_time=GETDATE();
	print'Total Loading Batch Duration: '+ cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+' seconds';
	END TRY
	BEGIN CATCH
		print'error message'+ERROR_MESSAGE();
		print'error message'+cast(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END
