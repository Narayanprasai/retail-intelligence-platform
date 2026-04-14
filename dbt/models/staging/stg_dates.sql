WITH source AS (
    SELECT * FROM {{ source('raw_data', 'dim_date') }}
),

cleaned AS (
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
    FROM source
    WHERE full_date IS NOT NULL
)

SELECT * FROM cleaned