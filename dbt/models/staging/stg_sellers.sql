WITH source AS (
    SELECT * FROM {{ source('raw_data', 'dim_seller') }}
),

cleaned AS (
    SELECT
        seller_sk,
        seller_id,
        LOWER(TRIM(seller_city))  AS seller_city,
        UPPER(TRIM(seller_state)) AS seller_state,
        seller_zip_code_prefix,
        CURRENT_TIMESTAMP         AS dbt_loaded_at
    FROM source
    WHERE seller_id IS NOT NULL
)

SELECT * FROM cleaned