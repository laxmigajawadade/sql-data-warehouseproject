/*
==============================
Create tables in bronze layer
==============================
In bronze layer, we are copying the data from the source systems as is(raw data). 
Source systems for this project are '.csv' files from 'crm' and 'erp'.
->data related to customers, products, sales come from crm
->data from erp. 


Warning: below script deletes the tables and their dependencies(views,indexes,tables etc.,) if already existing.
Please proceed with caution
*/

drop table  if exists bronze.crm_cust_info cascade; --also drops dependent objects (views, foreign keys, etc.)
create table bronze.crm_cust_info
(
cust_id int,
cust_key varchar,
cust_firstname varchar,
cust_lastname varchar,
cust_marital_status varchar,
cust_gender varchar,
cust_create_date date
);

drop table  if exists bronze.crm_product_info cascade; 
create table bronze.crm_product_info
(
prd_id int,
prod_key varchar,
prod_name varchar,
prod_cost int,
prod_line varchar,
prod_startdate date,
prod_enddate date
);

drop table if exists bronze.crm_sales_details cascade;
create table bronze.crm_sales_details
(
sls_ordnum varchar,
sls_prod_key varchar,
sls_cust_id int,
sls_order_date varchar,
sls_ship_date varchar,
sls_due_date varchar,
sls_sales int,
sls_qty int,
sls_price int
);

drop table if exists bronze.erp_cust_az12 cascade;
create table bronze.erp_cust_az12
(
cid varchar,
bdate date,
gen varchar
);

drop table if exists bronze.erp_loc_a101 cascade;
create table bronze.erp_loc_a101
(
cid varchar,
cntry varchar
);

drop table if exists bronze.erp_px_cat_g1v2 cascade;
create table bronze.erp_px_cat_g1v2
(
id varchar,
cat varchar,
subcat varchar,
maintainance varchar
);
