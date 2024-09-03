CREATE OR REPLACE TABLE `{prod_dataset}.dim_customers` AS 
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM `{stg_dataset}.customers`;