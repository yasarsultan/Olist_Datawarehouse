CREATE OR REPLACE TABLE `{prod_dataset}.dim_sellers` AS
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM `{stg_dataset}.sellers`;