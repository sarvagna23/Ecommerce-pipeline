WITH source AS (
    SELECT * FROM ecommerce.customers
),

cleaned AS (
    SELECT
        customer_id,
        age,
        region,
        CAST(signup_date AS DATE) AS signup_date,
        is_premium,
        CASE
            WHEN age < 25 THEN 'Gen Z'
            WHEN age < 40 THEN 'Millennial'
            WHEN age < 55 THEN 'Gen X'
            ELSE 'Boomer'
        END AS age_segment
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned