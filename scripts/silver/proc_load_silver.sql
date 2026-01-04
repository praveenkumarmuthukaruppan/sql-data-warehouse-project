/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

create or alter procedure silver.load_silver as 
begin
       declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
       BEGIN TRY
	    set @batch_start_time=GETDATE();
        print'=========================================';
	    print'Loading bronze layer';
	    print'=========================================';
	
	    print'-----------------------------------------';
	    set @start_time=GETDATE();
        print'>>Truncating table:silver.crm_prd_info';
        Truncate table silver.crm_prd_info;
        print'>>inserting table:silver.crm_prd_info';
        insert into silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt)
        select 
        prd_id,
        replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,--extract category Id
        substring(prd_key,7,LEN(prd_key)) as prd_key,--extract product key
        prd_nm,
        isnull(prd_cost,0) as prd_cost,
        case
             when upper(trim(prd_line))='M' then 'Mountain'
             when upper(trim(prd_line))='R' then 'Road'
             when upper(trim(prd_line))='S' then 'Other Sales'
             when upper(trim(prd_line))='T' then 'Touring'
             else 'n/a'
        end as prd_line,--Map product line codes to descriptive values
        cast(prd_start_dt as date) as prd_start_dt,
        cast(
            LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 
            as date
           )as prd_end_dt --calculate end date as one day before the next start date
        from[bronze].[crm_prd_info]; 
        set @end_time = GETDATE();
	    print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';

        
	    print'--------------------------------------------';
	    set @start_time=GETDATE();
        print'>>Truncating table:silver.erp_px_cat_g1v2';
        Truncate table silver.erp_px_cat_g1v2;
        print'>>inserting table:silver.erp_px_cat_g1v2';
        insert into silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance)
        select 
        id,
        cat,
        subcat,
        maintenance
        from [bronze].[erp_px_cat_g1v2];
        set @end_time = GETDATE();
	    print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';


        print'--------------------------------------------';
	    set @start_time=GETDATE();
        print'>>Truncating table:silver.crm_cust_info';
        Truncate table silver.crm_cust_info;
        print'>>inserting table:silver.crm_cust_info';
        insert into silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date)

        SELECT cst_id,
               cst_key,
               Trim(cst_firstname) as cst_firstname,
               Trim(cst_lastname) as cst_lastname,
               case when upper(trim(cst_marital_status))='S' then 'Single'
                    when upper(trim(cst_marital_status))='M' then 'Married'
                    else 'n/a'
               end  cst_marital_status,
               case when upper(trim(cst_gndr))='M' then 'Male'
                    when upper(trim(cst_gndr))='F' then 'Female'
                    else 'n/a'
               end cst_gndr,
               cst_create_date
          FROM (
                select *,
                ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
                from bronze.crm_cust_info
                where cst_id is not null
            )t where flag_last=1;
            set @end_time = GETDATE();
	        print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';

        print'--------------------------------------------';
	    set @start_time=GETDATE();
        print'>>Truncating table:silver.erp_cust_az12';
        Truncate table silver.erp_cust_az12;
        print'>>inserting table:silver.erp_cust_az12';
        insert into silver.erp_cust_az12(
        cid,
        bdate,
        gen)
        select 
        case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
	         else cid
        end as cid,
        case when bdate >GETDATE() then null
             else bdate
        end as bdate,
        case when upper(trim(gen)) in ('F','Female') then 'Female'
             when upper(trim(gen)) in ('M','Male') then 'Male'
	         else 'n/a'
        end as gen
        from [bronze].[erp_cust_az12];
        set @end_time = GETDATE();
	    print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';


        print'--------------------------------------------';
	    set @start_time=GETDATE();
        print'>>Truncating table:silver.crm_sales_details';
        Truncate table silver.crm_sales_details;
        print'>>inserting table:silver.crm_sales_details';
        insert into silver.crm_sales_details (
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
        select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt =0 or LEN(sls_order_dt)!=8 then null
	         else cast(cast(sls_order_dt as varchar)as date)	
        end as sls_order_dt, 
        case when sls_ship_dt =0 or len(sls_ship_dt)!=8 then null
             else cast(cast(sls_ship_dt as varchar) as date)
        end as sls_ship_dt,
        case when sls_due_dt =0 or len(sls_due_dt)!=8 then null
             else cast(cast(sls_due_dt as varchar) as date)
        end as sls_due_dt,
        case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity * ABS(sls_price) 
		         then sls_quantity * ABS(sls_price)
	         else sls_sales
        end as sls_sales,
        sls_quantity,
        case when sls_price is null or sls_price <=0
                  then sls_sales/nullif(sls_quantity,0)
	         else sls_price
        end as sls_price
        from bronze.[crm_sales_details];
        set @end_time = GETDATE();
	    print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';


        print'--------------------------------------------';
	    set @start_time=GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
		        TRUNCATE TABLE silver.erp_loc_a101;
		        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		        INSERT INTO silver.erp_loc_a101 (
			        cid,
			        cntry
		        )
		        SELECT
			        REPLACE(cid, '-', '') AS cid, 
			        CASE
				        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				        ELSE TRIM(cntry)
			        END AS cntry -- Normalize and Handle missing or blank country codes
		        FROM bronze.erp_loc_a101;
                set @end_time = GETDATE();
	    print'>> Load duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+' seconds';
        print'-----------------------------------------------------------';
	print'Loading bronze layer is completed';
	set @batch_end_time=GETDATE();
	print'Total Loading Batch Duration: '+ cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+' seconds';
	END TRY
	BEGIN CATCH
		print'error message'+ERROR_MESSAGE();
		print'error message'+cast(ERROR_NUMBER() AS NVARCHAR);
	END CATCH

end
