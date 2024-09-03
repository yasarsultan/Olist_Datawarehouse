CREATE OR REPLACE TABLE `{prod_dataset}.fact_orders` AS
WITH 

    aggregated_order_items AS (
        SELECT
            order_id,
            ARRAY_AGG(seller_id ORDER BY seller_id)[OFFSET(0)] AS seller_id,
            ARRAY_AGG(product_id ORDER BY product_id)[OFFSET(0)] AS product_id,
            SUM(price) AS total_order_amount,
            SUM(freight_value) AS total_freight_value
        FROM `{stg_dataset}.order_items`
        GROUP BY order_id
    ),

    aggregated_payments AS (
        SELECT
            order_id,
            ARRAY_AGG(payment_type ORDER BY payment_type)[OFFSET(0)] AS payment_type,
            SUM(payment_value) AS payment_value
        FROM `{stg_dataset}.order_payments`
        GROUP BY order_id
    ),

    aggregated_reviews AS (
        SELECT
            order_id,
            AVG(review_score) AS review_score
        FROM `{stg_dataset}.order_reviews`
        GROUP BY order_id
    )

SELECT
    orders.order_id,
    orders.customer_id,
    COALESCE(items.seller_id, 'unknown') AS seller_id,
    COALESCE(items.product_id, 'unknown') AS product_id,
    orders.order_status,
    DATE(orders.order_purchase_timestamp) AS order_date,
    COALESCE(payments.payment_type, 'unknown') AS payment_type,
    COALESCE(payments.payment_value, 0) AS payment_value,
    COALESCE(items.total_freight_value, 0) AS freight_value,
    COALESCE(reviews.review_score, NULL) AS review_score,
    COALESCE(items.total_order_amount, 0) AS total_order_amount,  
    DATE(orders.order_delivered_customer_date) AS delivery_date, 
    TIMESTAMP_DIFF(TIMESTAMP(orders.order_approved_at), TIMESTAMP(orders.order_purchase_timestamp), HOUR) AS hours_to_approve,  -- Difference in hours
    TIMESTAMP_DIFF(DATE(orders.order_delivered_customer_date), DATE(orders.order_purchase_timestamp), DAY) AS days_to_deliver,  -- Difference in days
    TIMESTAMP(orders.order_approved_at) AS approved_at,
    TIMESTAMP(orders.order_delivered_customer_date) AS delivered_at
FROM `{stg_dataset}.orders` AS orders
LEFT JOIN aggregated_order_items AS items
    ON orders.order_id = items.order_id
LEFT JOIN aggregated_payments AS payments
    ON orders.order_id = payments.order_id
LEFT JOIN aggregated_reviews AS reviews
    ON orders.order_id = reviews.order_id;
