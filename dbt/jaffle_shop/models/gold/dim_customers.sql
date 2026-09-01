with customers as (
    select * from {{ ref('customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

customer_orders as (
    select
        customer,
        min(ordered_at) as first_order_date,
        max(ordered_at) as most_recent_order_date,
        count(id) as number_of_orders
    from orders
    group by 1
),

final as (
    select
        c.id,
        c.name,
        co.first_order_date,
        co.most_recent_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders
    from customers c
    left join customer_orders co on co.customer = c.id
)

select * from final