{#
    Customer dimension. Grain: one row per REAL person (source customer_unique_id).
    In gold, customer_id = the person.

    IMPORTANT: the source has no customer master data (no CRM / registered address).
    The source "customers" table is a per-order record: who ordered and WHERE IT WAS DELIVERED.
    -> The exact delivery address belongs to gold.order (delivery_*).
    -> Here we only keep the LAST delivery location, named honestly as such,
       for rough "customers by region" questions.
#}

with customers as (

    select * from {{ ref('stg_olist__customers') }}

),

orders as (

    select order_id, customer_id, ordered_at from {{ ref('stg_olist__orders') }}

),

-- each source customer_id = one order -> gives the person's orders and their delivery locations
customer_orders as (

    select
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.ordered_at
    from customers c
    join orders o on o.customer_id = c.customer_id

),

-- delivery location of the person's most recent order (not a registered address)
last_delivery as (

    select *
    from customer_orders
    qualify row_number() over (partition by customer_unique_id order by ordered_at desc) = 1

),

-- roll-up to the dimension's own grain (one person)
order_stats as (

    select
        customer_unique_id,
        min(ordered_at)                             as first_order_at,
        max(ordered_at)                             as last_order_at,
        count(*)                                    as order_count,
        count(distinct customer_zip_code_prefix)    as delivery_zip_count   -- > 1: orders shipped to several places
    from customer_orders
    group by customer_unique_id

),

geo as (

    select * from {{ ref('int_geolocation__zip_prefix') }}

)

select
    l.customer_unique_id                as customer_id,

    -- last delivery location (explicit naming: this is NOT a registered home address)
    l.customer_zip_code_prefix          as last_delivery_zip_code_prefix,
    initcap(l.customer_city)            as last_delivery_city,
    l.customer_state                    as last_delivery_state,
    g.latitude                          as last_delivery_latitude,
    g.longitude                         as last_delivery_longitude,

    -- order history
    s.first_order_at,
    s.last_order_at,
    s.order_count,
    s.order_count > 1                   as is_repeat_customer,
    s.delivery_zip_count

from last_delivery l
join order_stats s using (customer_unique_id)
left join geo g on g.zip_code_prefix = l.customer_zip_code_prefix