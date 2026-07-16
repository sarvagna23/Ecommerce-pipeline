WITH source AS (
    SELECT * FROM ecommerce.orders
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        discount,
        total,
        CAST(order_date AS TIMESTAMP) AS order_date,
        status,
        CASE
            WHEN status = 'completed' THEN TRUE
            ELSE FALSE
        END AS is_completed,
        CASE
            WHEN status = 'returned' THEN TRUE
            ELSE FALSE
        END AS is_returned
    FROM source
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND total > 0
)

SELECT * FROM cleaned