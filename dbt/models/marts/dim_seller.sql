WITH stg AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)

SELECT
    seller_sk,
    seller_id,
    seller_city,
    seller_state,
    seller_zip_code_prefix,
    dbt_loaded_at
FROM stg