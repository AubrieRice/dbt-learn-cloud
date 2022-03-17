with

orders as (
    select * from {{ source('jaffle_shop', 'orders') }}
), 

payments as (
    select * from {{ source('stripe', 'payment') }}
), 

customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

successful_payment_orders as (
    
    select
        orderid as order_id
        , max(created) as payment_finalized_date
        , sum(amount) / 100.0 as total_amount_paid
    
    from payments
    where status <> 'fail'
    group by 1
),

customer_paid_orders_and_ltv as (
    
    select
        paid_orders.order_id
        , sum(ltv.total_amount_paid) as customer_lifetime_value
    from paid_orders
    left join paid_orders ltv on paid_orders.customer_id = ltv.customer_id 
        and paid_orders.order_id >= ltv.order_id
    group by 1
    order by paid_orders.order_id

),

paid_orders as (
    
    select
        orders.id as order_id
        , orders.user_id as customer_id
        , orders.order_date as order_placed_at
        , orders.status as order_status
        , p.total_amount_paid
        , p.payment_finalized_date
        , c.first_name as customer_first_name
        , c.last_name as customer_last_name

    from orders as orders
    left join successful_payment_orders p on orders.id = p.order_id
    left join customers c on orders.user_id = c.id
),

customer_orders as (
    
    select
        c.id as customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(orders.id) as number_of_orders
    
    from customers c 
    left join orders as orders on orders.user_id = c.id 
    group by 1
)

select
    p.*
    , row_number() over (order by p.order_id) as transaction_seq
    , row_number() over (
        partition by customer_id order by p.order_id) as customer_sales_seq
    , case when c.first_order_date = p.order_placed_at
        then 'new'
        else 'return'
        end as nvsr
    , x.customer_lifetime_value
    , c.first_order_date as fdos

from paid_orders p
left join customer_orders as c using (customer_id)
left outer join customer_paid_orders_and_ltv x on x.order_id = p.order_id
order by order_id