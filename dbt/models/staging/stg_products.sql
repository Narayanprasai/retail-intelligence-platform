WITH source AS (
    SELECT * FROM {{ source('raw_data', 'dim_product') }}
),

cleaned AS (
    SELECT
        product_sk,
        product_id,
        LOWER(TRIM(product_category_name)) AS product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        product_photos_qty,
        CURRENT_TIMESTAMP                  AS dbt_loaded_at
    FROM source
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned