/*
===============================================================================
Data Quality Checks: Gold Layer
===============================================================================
Script Purpose:

    This script performs basic data quality validation
    on Gold layer dimension and fact views.

    It checks the uniqueness of surrogate keys in
    customer and product dimensions and verifies
    referential integrity between the fact and
    dimension views.

    The script helps ensure that the Gold layer
    data model is consistent and ready for reporting.
===============================================================================
*/

--quality check

--gold.dim_customes
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
select customer_key,count(*) as duplicate_count
from gold.dim_customers group by customer_key
having count(*)>1

--gold.product_key
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
select product_key,count(*) as duplicate_count
from gold.dim_products 
group by product_key 
having count(*)>1

--gold.fact_sales
--foreign key integrity 
-- Check the data model connectivity between fact and dimensions

select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null or c.customer_key is null




