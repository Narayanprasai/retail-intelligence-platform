WITH stg AS (
    SELECT * FROM {{ ref('stg_dates') }}
)

SELECT
    date_id,
    full_date,
    year,
    month,
    quarter,
    day_of_week,
    day_name,
    month_name,
    is_weekend
FROM stg