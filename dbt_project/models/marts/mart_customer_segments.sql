WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT
        o.customer_id,
        c.region,
        c.is_premium,
        c.age_segment,
        SUM(o.total)            AS lifetime_value,
        COUNT(o.order_id)       AS total_orders,
        AVG(o.total)            AS avg_order_value,
        SUM(CASE WHEN o.is_returned THEN 1 ELSE 0 END) AS total_returns,
        MIN(o.order_date)       AS first_order_date,
        MAX(o.order_date)       AS last_order_date
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY 1, 2, 3, 4
),

segmented AS (
    SELECT
        *,
        CASE
            WHEN lifetime_value > 50000 THEN 'VIP'
            WHEN lifetime_value > 20000 THEN 'High Value'
            WHEN lifetime_value > 5000  THEN 'Mid Value'
        ELSE 'Low Value'
END AS customer_segment
    FROM customer_metrics
)

SELECT * FROM segmented