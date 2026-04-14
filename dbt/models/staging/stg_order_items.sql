WITH source AS (
    SELECT * FROM {{ source('raw_data', 'fact_order_items') }}
),

cleaned AS (
    SELECT
        order_id,
        order_item_id,
        LOWER(TRIM(order_status))   AS order_status,
        customer_sk,
        product_sk,
        seller_sk,
        date_id,
        price,
        freight_value,
        total_amount,
        delivery_days,
        is_late_delivery,
        avg_review_score,
        LOWER(TRIM(payment_type))   AS payment_type,
        CURRENT_TIMESTAMP           AS dbt_loaded_at
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned