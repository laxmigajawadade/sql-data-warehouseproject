/*
==========================================
load data from souce csv to bronze tables
==========================================

Through this script we are loading the data from crm and erp '.csv' files to database
We are using full extraction and 'Truncate and insert' Mechanism. 

Warning:
data is truncated from tables first and then new data is inserted
please proceed with caution
*/



/* 
In PostgreSQL, changes are first recorded in WAL (Write-Ahead Log).
WAL records are written to the WAL buffer (in RAM).

When COMMIT is executed:

With synchronous_commit = on (default):
WAL records (including the COMMIT record) are written from WAL buffer to WAL files on disk
and flushed (fsync). PostgreSQL waits until the disk confirms the flush.
Only then is success returned to the application.

This guarantees durability — even if power fails immediately after commit,
the transaction can be recovered using WAL.

With synchronous_commit = off:
WAL records are written to WAL buffer.
At COMMIT, PostgreSQL does NOT wait for WAL flush to disk.
Success is returned immediately.
WAL is flushed to disk shortly after in the background.

This improves performance for large batch operations like COPY.

However, if the server crashes before WAL is flushed,
the committed transaction may be lost.

For batch loads or re-runnable jobs, synchronous_commit = off
can be acceptable because the operation can be executed again.
*/

begin;	
set local synchronous_commit = off;
	truncate table crm_cust_info;
	\copy  crm_cust_info
	from 'D:\\vaarahi\\SQL Project\\data with baara proj\\sql-data-warehouse-project-main\\datasets\\source_crm\\cust_info.csv'
	with
	(
	header true,	--treat the first row as header 
	delimiter ',',  --csv file so seperate data at commas
	null ''  		--if empty field in the CSV (nothing between delimiters), treat it as NULL
	);	

	truncate table crm_product_info;
	\copy crm_product_info
	from 'D:\vaarahi\SQL Project\data with baara proj\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	with 
	(
	header true,
	delimiter ',',
	null ''
	);
	

	truncate table crm_sales_details;
	\copy crm_sales_details 
	from 'D:\vaarahi\SQL Project\data with baara proj\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	with 
	(
		header true,
		delimiter ',',
		null ''
	);
	
	truncate table bronze.erp_cust_az12;
	\copy bronze.erp_cust_az12 
	from 'D:\vaarahi\SQL Project\data with baara proj\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
	with ( header true, delimiter ',', null '');
	
	
	truncate table bronze.erp_loc_a101;
	\copy bronze.erp_loc_a101 
	from 'D:\vaarahi\SQL Project\data with baara proj\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
	with ( header true, delimiter ',', null '');
	

	truncate table bronze.erp_px_cat_g1v2;
	\copy bronze.erp_px_cat_g1v2 
	from 'D:\vaarahi\SQL Project\data with baara proj\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
	with ( header true, delimiter ',', null '');
	
commit;