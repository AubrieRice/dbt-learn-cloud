{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}

WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
), 

pivoted AS (
    SELECT
        order_id
        {% for payment_method in payment_methods -%}
        , sum(CASE WHEN payment_method = '{{ payment_method }}' THEN amount ELSE 0 END) AS {{payment_method}}
        {% endfor %}

    FROM payments
    WHERE status = 'success'
    GROUP BY 1
)

SELECT * FROM pivoted