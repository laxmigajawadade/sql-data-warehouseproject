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

create or replace procedure bronze.load_bronze()
language plpgsql
as $$
declare rows_inserted integer;
begin
	 
	raise notice '========================================';
	raise notice 'loading data into bronze tables: start';
	raise notice '========================================';
	set local synchronous_commit = off;

	begin --try block
		raise notice '----------------------------------------------';
		raise notice 'truncating table crm_cust_info';
		truncate table bronze.crm_cust_info;
		raise notice 'loading data from crm to crm_cust_info table';
		copy  bronze.crm_cust_info
		from '/app/datasets/source_crm/cust_info.csv'
			with
			(
			header true,	--treat the first row as header 
			delimiter ',',  --csv file so seperate data at commas
			null ''  		--if empty field in the CSV (nothing between delimiters), treat it as NULL
			);	
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE 'Rows inserted: %', rows_inserted;
		raise notice 'load complete';
		raise notice '----------------------------------------------';

		raise notice '----------------------------------------------';
		raise notice 'truncating table crm_product_info';
		truncate table bronze.crm_product_info;
		raise notice 'loading data from crm to crm_product_info table';
		copy bronze.crm_product_info
		from '/app/datasets/source_crm/prd_info.csv'
			with 
			(
			header true,
			delimiter ',',
			null ''
			);
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE 'Rows inserted: %', rows_inserted;
		raise notice 'load complete to crm_product_info';
		raise notice '----------------------------------------------';


		raise notice '----------------------------------------------';
		raise notice 'truncating table crm_sales_details';
		truncate table bronze.crm_sales_details;
		raise notice 'loading data from crm to crm_sales_details table';
		copy bronze.crm_sales_details 
		from '/app/datasets/source_crm/sales_details.csv'
			with 
			(
				header true,
				delimiter ',',
				null ''
			);	
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE 'Rows inserted: %', rows_inserted; 
		raise notice 'load complete to crm_sales_details';
		raise notice '----------------------------------------------';


		raise notice '----------------------------------------------';
		raise notice 'truncating table erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		raise notice 'loading data from erp to erp_cust_az12 table';
		copy bronze.erp_cust_az12 
		from '/app/datasets/source_erp/CUST_AZ12.csv'
			with 
			( 
				header true, 
				delimiter ',', 
				null ''
			);
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE 'Rows inserted: %', rows_inserted;
		raise notice 'load complete to erp_cust_az12';	
		raise notice '----------------------------------------------';


		raise notice '----------------------------------------------';
		raise notice 'truncating table erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		raise notice 'loading data from erp to erp_loc_a101 table';
		copy bronze.erp_loc_a101 
		from '/app/datasets/source_erp/LOC_A101.csv'
			with 
			( 
				header true, 
				delimiter ',', 
				null ''
			);
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE 'Rows inserted: %', rows_inserted;
		raise notice 'load complete to erp_loc_a101';	
		raise notice '----------------------------------------------';


		raise notice '----------------------------------------------';
		raise notice 'truncating table erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		raise notice 'loading data from erp to erp_px_cat_g1v2 table';
		copy bronze.erp_px_cat_g1v2 
		from '/app/datasets/source_erp/PX_CAT_G1V2.csv'
			with
			( 
				header true, 
				delimiter ',',
				null ''
			);
		GET DIAGNOSTICS rows_inserted = ROW_COUNT;
		RAISE NOTICE 'Rows inserted: %', rows_inserted;
		raise notice 'load complete to erp_px_cat_g1v2';
		raise notice '----------------------------------------------';

		raise notice '======================================================';
		raise notice 'Full load(truncate&Load) completed into bronze tables';
		raise notice '======================================================';
	exception 
		when OTHERS then 
			raise notice 'error code: %',SQLSTATE;
			raise notice 'error is%',sqlerrm;
			raise;
	end;
end;
$$;
