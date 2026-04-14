WITH stg AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_sk,
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    valid_from,
    valid_to,
    is_current,
    dbt_loaded_at
FROM stg