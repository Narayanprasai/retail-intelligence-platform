WITH source AS (
    SELECT * FROM {{ source('raw_data', 'dim_customer') }}
),

cleaned AS (
    SELECT
        customer_sk,
        customer_id,
        customer_unique_id,
        LOWER(TRIM(customer_city))  AS customer_city,
        UPPER(TRIM(customer_state)) AS customer_state,
        valid_from,
        valid_to,
        is_current,
        CURRENT_TIMESTAMP           AS dbt_loaded_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned