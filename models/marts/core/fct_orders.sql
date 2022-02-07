WITH orders AS (
    SELECT
        *
    FROM {{ ref('stg_orders') }}
), 

payments AS (
    SELECT
        *
    FROM {{ ref('stg_payments') }}
), 

order_payments AS (
    SELECT
        order_id
        , sum(case when status = 'success' then amount end) as amount
    FROM payments
    GROUP BY 1
),

final AS (
    SELECT
        orders.order_id
        , orders.customer_id
        , orders.order_date
        , coalesce(order_payments.amount, 0) as amount
    FROM orders
    LEFT JOIN order_payments using (order_id)
)

SELECT * FROM final