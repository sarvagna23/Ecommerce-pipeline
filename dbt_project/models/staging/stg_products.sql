WITH source AS (
    SELECT * FROM ecommerce.products
),

cleaned AS (
    SELECT
        product_id,
        category,
        base_price,
        cost,
        inventory,
        ROUND(base_price - cost, 2)              AS gross_margin,
        ROUND((base_price - cost) / base_price, 3) AS margin_rate
    FROM source
    WHERE product_id IS NOT NULL
      AND base_price > 0
)

SELECT * FROM cleaned