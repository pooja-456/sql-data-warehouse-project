/*
======================================================================
Stored Procedure: Load Bronze Layer Data (Source->Bronze)
======================================================================
Script Purpose:

    This script creates or alters the stored procedure
    'bronze.load_bronze'.

    The procedure loads raw data from CRM and ERP source
    CSV files into the Bronze layer tables.

    It truncates existing data, performs bulk inserts,
    tracks load duration for each table, and logs the
    total batch execution time.

    Error handling is included to capture and display
    any issues during the load process.
======================================================================
*/


create or alter procedure bronze.load_bronze as 
begin
    declare @start_time datetime , @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
    begin try

	 set @batch_start_time=getdate();
     print replicate ('=',50);
     print 'Loading Bronze Layer';
	 print replicate('=',50);

	 print replicate('-',50);
	 print 'Loading CRM Tables';
	 print replicate('-',50);

     set @start_time=getdate();
	 print '>> Truncating Table: bronze.crm_cust_info';
	 truncate table bronze.crm_cust_info;

	 print '>> Inserting Data Into: bronze.crm_cust_info';
	 bulk insert bronze.crm_cust_info
	 from 'C:\Users\itsme\OneDrive\Desktop\bigdataprojects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	 with (
	  firstrow=2,
	  fieldterminator=',',
	  tablock
	);
     set @end_time=getdate();
	 print '>> Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
	 print replicate('-',50)


     
	set @start_time=getdate();
    print '>> Truncating Table: bronze.crm_prd_info';
    truncate table bronze.crm_prd_info;

    print '>> Inserting Data Into: bronze.crm_prd_info';
    bulk insert bronze.crm_prd_info
	from 'C:\Users\itsme\OneDrive\Desktop\bigdataprojects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
	 firstrow=2,
	 fieldterminator=',',
	 tablock
	);
     set @end_time=getdate();
	 print '>> Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
	 print replicate('-',50)
    


	set @start_time=getdate();
	print '>> Truncating Table: bronze.crm_sales_details';
    truncate table bronze.crm_sales_details;

	print '>> Inserting Data Into: bronze.crm_sales_details';
    bulk insert bronze.crm_sales_details
	from 'C:\Users\itsme\OneDrive\Desktop\bigdataprojects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
	 firstrow=2,
	 fieldterminator=',',
	 tablock
	);
     set @end_time=getdate();
	 print '>> Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
	 print replicate('-',50)


	print replicate('-',50);
	print 'Loading ERP Tables';
	print replicate('-',50);



	set @start_time=getdate();
    print '>> Truncating Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101;

	print '>> Inserting Data Into: bronze.erp_loc_a101';
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\itsme\OneDrive\Desktop\bigdataprojects\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with (
	 firstrow=2,
	 fieldterminator=',',
	 tablock
	);
     set @end_time=getdate();
	 print '>> Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
	 print replicate('-',50)




    set @start_time=getdate();
    print '>> Truncating Table: bronze.erp_cust_az12';
    truncate table bronze.erp_cust_az12;

    print '>> Inserting Data Into: bronze.erp_cust_az12';
    bulk insert bronze.erp_cust_az12
    from 'C:\Users\itsme\OneDrive\Desktop\bigdataprojects\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with (
	 firstrow=2,
	 fieldterminator=',',
	 tablock
	);
     set @end_time=getdate();
	 print '>> Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
	 print replicate('-',50)



	set @start_time=getdate();
    print '>> Truncating Table: bronze.erp_px_cat_g1v2';
    truncate table bronze.erp_px_cat_g1v2;

	print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\itsme\OneDrive\Desktop\bigdataprojects\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with (
	 firstrow=2,
	 fieldterminator=',',
	 tablock
	);
	 set @end_time=getdate();
	 print '>> Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
	 print replicate('-',50)

	 set @batch_end_time =getdate();
	 print replicate('=',50)
	 print 'Loading Bronze Layer is Completed';
	 print 'Total Load Duration: '+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+ ' seconds';
	 print replicate('=',50)

	end try
	begin catch
	 print replicate('=',50)
	 print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	 print 'Error Message'+error_message();
	 print replicate('=',50)
	end catch
end
go
exec bronze.load_bronze


