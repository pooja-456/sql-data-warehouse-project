/*
======================================================================
Data Exploration and Quality Checks (Bronze & Silver Layers)
======================================================================

Script Purpose:

    This script explores data in the Bronze and Silver
    layers and performs basic data quality checks.

    It helps validate data consistency and correctness
    after transformation from Bronze to Silver.
======================================================================
*/

--Explore & understand the data in bronze layer

--focus: customer
--data contains: customer Information
select top 1000 * from bronze.crm_cust_info

--data contains: Extra customer details
select top 1000 * from bronze.erp_cust_az12

-- data contains: customer data with country 
select top 1000 * from bronze.erp_loc_a101



--focus: product
-- data contains:Current & History product data
select top 1000 * from bronze.crm_prd_info

--data conatins: product info with categories details
select top 1000 * from bronze.erp_px_cat_g1v2


-- focus: sales 
--data contains: Transactional records about sales & orders
select top 1000 * from bronze.crm_sales_details




--bronze.crm_cust_info
-- check for nulls or duplicates in primary
-- Expected Outcome: No result

select cst_id, count(*)
from bronze.crm_cust_info 
group by cst_id 
having count(*)>1 or cst_id is null


-- check for unwanted spaces
--Expected Outcome : No result

--firstname
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)


--lastname
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)


--gender
select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)


--data standardization & consistency
--gender
select distinct cst_gndr
from bronze.crm_cust_info

--marital_status
select distinct cst_marital_status
from bronze.crm_cust_info




-- quality check 

-- check for nulls or duplicates in primary
-- Expected Outcome: No result

select cst_id, count(*)
from silver.crm_cust_info 
group by cst_id 
having count(*)>1 or cst_id is null


-- check for unwanted spaces
--Expected Outcome : No result

--firstname
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)


--lastname
select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)


--gender
select cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr)


--data standardization & consistency
--gender
select distinct cst_gndr
from silver.crm_cust_info

--marital_status
select distinct cst_marital_status
from silver.crm_cust_info

-- silver.crm_cust_info
select * from silver.crm_cust_info




--bronze.crm_prd_info
--check for nulls or duplicates in primary key
--Expected Output: No result


select prd_id,count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null

--check for unwanted spaces 
--Expected outcome: No result

select prd_nm
from bronze.crm_prd_info
where prd_nm!=trim(prd_nm)


--check for nulls or negative numbers
--Expected outcomes: No result

select prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null

--data standardization & consistency

select distinct prd_line
from bronze.crm_prd_info

--check for invalid date orders
select * 
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt




--quality check

--check for nulls or duplicates in primary key
--Expected Output: No result


select prd_id,count(*)
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null

--check for unwanted spaces 
--Expected outcome: No result

select prd_nm
from silver.crm_prd_info
where prd_nm!=trim(prd_nm)


--check for nulls or negative numbers
--Expected outcomes: No result

select prd_cost
from silver.crm_prd_info
where prd_cost<0 or prd_cost is null

--data standardization & consistency

select distinct prd_line
from silver.crm_prd_info

--check for invalid date orders
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

--silver.crm_prd_info
select * from silver.crm_prd_info




--bronze.crm_sales_details

--check for unwanted spaces 
--Expected outcome: No result
select * from bronze.crm_sales_details where sls_ord_num != trim(sls_ord_num)

--data integrity
-- sls_prd_key (sales) , prd_key (crm_prd_info)
select * 
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

-- sls_cust_id (sales) , cst_id(crm_cust_info)
select *
from bronze.crm_sales_details
where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info) 

--check for invalid dates
--sls_order_dt
select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0 
or len(sls_order_dt)!=8
or sls_order_dt >20500101 or sls_order_dt< 19000101

--sls_ship_dt
select 
nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt<=0 
or len(sls_ship_dt)!=8
or sls_ship_dt >20500101 or sls_ship_dt< 19000101

--sls_due_dt

select 
nullif(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt<=0 
or len(sls_due_dt)!=8
or sls_due_dt >20500101 or sls_due_dt< 19000101


-- check for invalid date orders
select *
from bronze.crm_sales_details
where sls_order_dt> sls_ship_dt or sls_order_dt >sls_due_dt

--check data consistency : between sales, quantity and price
--sales =quantity*price
--value must not be null , zero or negative

select distinct 
sls_sales ,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity *sls_price
or sls_sales is null 
or sls_quantity is null 
or sls_price is null
or sls_sales <=0 
or sls_quantity <=0 
or sls_price <=0
order by sls_sales, sls_quantity,sls_price

--quantity check


--check for unwanted spaces 
--Expected outcome: No result
select * from silver.crm_sales_details where sls_ord_num != trim(sls_ord_num)

--data integrity
-- sls_prd_key (sales) , prd_key (crm_prd_info)
select * 
from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

-- sls_cust_id (sales) , cst_id(crm_cust_info)
select *
from silver.crm_sales_details
where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info) 

--check for invalid dates
--sls_order_dt
select 
nullif(sls_order_dt,0) sls_order_dt
from silver.crm_sales_details
where sls_order_dt<=0 
or len(sls_order_dt)!=8
or sls_order_dt >20500101 or sls_order_dt< 19000101

--sls_ship_dt
select 
nullif(sls_ship_dt,0) sls_ship_dt
from silver.crm_sales_details
where sls_ship_dt<=0 
or len(sls_ship_dt)!=8
or sls_ship_dt >20500101 or sls_ship_dt< 19000101

--sls_due_dt

select 
nullif(sls_due_dt,0) sls_due_dt
from silver.crm_sales_details
where sls_due_dt<=0 
or len(sls_due_dt)!=8
or sls_due_dt >20500101 or sls_due_dt< 19000101


-- check for invalid date orders
select *
from silver.crm_sales_details
where sls_order_dt> sls_ship_dt or sls_order_dt >sls_due_dt

--check data consistency : between sales, quantity and price
--sales =quantity*price
--value must not be null , zero or negative

select distinct 
sls_sales ,
sls_quantity,
sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity *sls_price
or sls_sales is null 
or sls_quantity is null 
or sls_price is null
or sls_sales <=0 
or sls_quantity <=0 
or sls_price <=0
order by sls_sales, sls_quantity,sls_price

--silver.crm_sales_details
select * from silver.crm_sales_details

--bronze.erp_cust_az12

--identify out-of-range dates
--bdate
select distinct bdate from bronze.erp_cust_az12
where bdate <'1924-01-01' or bdate > getdate()

--data standardization & consistency
select distinct gen from bronze.erp_cust_az12


--quality check

--identify out-of-range dates
--bdate
select distinct bdate from silver.erp_cust_az12
where bdate <'1924-01-01' or bdate > getdate()

--data standardization & consistency
select distinct gen from silver.erp_cust_az12

--silver.erp_cust_az12
select * from silver.erp_cust_az12


--bronze.erp_loc_a101
--data standardization & consistency
select distinct cntry from bronze.erp_loc_a101

--quality check
--data standardization & consistency
select distinct cntry from silver.erp_loc_a101 order by cntry

--silver.erp_loc_a101
select * from silver.erp_loc_a101

--bronze.erp_px_cat_g1v2
select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2

--check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat !=trim(cat) or subcat !=trim(subcat)or maintenance !=trim(maintenance)

--data standardization and consistency
--cat
select distinct cat from bronze.erp_px_cat_g1v2

--subcat
select distinct subcat from bronze.erp_px_cat_g1v2

--maintenance
select distinct maintenance from bronze.erp_px_cat_g1v2


--silver.erp_px_cat_g1v2
select * from silver.erp_px_cat_g1v2

