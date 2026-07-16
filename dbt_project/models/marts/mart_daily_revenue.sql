WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

daily_revenue AS (
    SELECT
        DATE(o.order_date)      AS order_date,
        p.category,
        SUM(o.total)            AS daily_revenue,
        COUNT(o.order_id)       AS order_count,
        AVG(o.total)            AS avg_order_value,
        SUM(o.total) / COUNT(o.order_id) AS revenue_per_order
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE o.is_completed = TRUE
    GROUP BY 1, 2
    ORDER BY 1, 2
)

SELECT * FROM daily_revenue