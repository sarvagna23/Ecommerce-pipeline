WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

performance AS (
    SELECT
        o.product_id,
        p.category,
        p.base_price,
        p.margin_rate,
        SUM(CASE WHEN o.is_completed THEN o.total ELSE 0 END) AS revenue,
        COUNT(o.order_id)                                       AS total_orders,
        SUM(CASE WHEN o.is_returned  THEN 1 ELSE 0 END)        AS returns,
        ROUND(
            SUM(CASE WHEN o.is_returned THEN 1 ELSE 0 END) * 1.0
            / COUNT(o.order_id), 3
        )                                                       AS return_rate,
        SUM(CASE WHEN o.is_completed THEN o.quantity ELSE 0 END) AS units_sold
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    GROUP BY 1, 2, 3, 4
    ORDER BY revenue DESC
)

SELECT * FROM performance