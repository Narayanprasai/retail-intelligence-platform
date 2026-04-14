WITH stg AS (
    SELECT * FROM {{ ref('stg_order_items') }}
)

SELECT
    order_id,
    order_item_id,
    order_status,
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
    payment_type,
    dbt_loaded_at
FROM stg