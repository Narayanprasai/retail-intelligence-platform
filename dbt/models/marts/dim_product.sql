WITH stg AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_sk,
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty,
    dbt_loaded_at
FROM stg