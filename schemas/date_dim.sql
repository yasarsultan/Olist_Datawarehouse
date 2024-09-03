CREATE OR REPLACE TABLE `{prod_dataset}.dim_date` AS
SELECT
    DATE(order_purchase_timestamp) AS order_date,
    EXTRACT(DAY FROM TIMESTAMP(order_purchase_timestamp)) AS day,
    EXTRACT(MONTH FROM TIMESTAMP(order_purchase_timestamp)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP(order_purchase_timestamp)) AS year,
    EXTRACT(QUARTER FROM TIMESTAMP(order_purchase_timestamp)) AS quarter,
    FORMAT_TIMESTAMP('%A', TIMESTAMP(order_purchase_timestamp)) AS day_of_week,
    EXTRACT(WEEK FROM TIMESTAMP(order_purchase_timestamp)) AS week_number,
FROM `{stg_dataset}.orders`
GROUP BY order_date, year, month, day, quarter, day_of_week, week_number;