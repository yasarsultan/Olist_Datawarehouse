CREATE OR REPLACE TABLE `{prod_dataset}.dim_products` AS
SELECT
    product_id,
    products.product_category_name,
    pe.product_category_name_english AS category_name,
    product_name_lenght AS product_name_length,
    product_description_lenght AS description_length,
    product_photos_qty AS photos_quantity,
    product_weight_g AS weight,
    product_length_cm AS length,
    product_height_cm AS height,
    product_width_cm AS width
FROM `{stg_dataset}.products` AS products
LEFT JOIN `{stg_dataset}.product_category_name_translation` AS pe
    ON products.product_category_name = pe.product_category_name;