{#
    Sales order fact. Grain: one row per order.
    Named sales_order because ORDER is a reserved SQL keyword.
    Order-level metrics live here (delivery time, delay, average order value, reviews) -
    they must NOT be averaged from order_item, where an order with 3 units counts 3 times.

    Fan-out protection: items, payments and reviews are each aggregated to ONE ROW PER ORDER
    in their own CTE first, and only then joined. Items and payments are never joined directly.
    Amounts are summed from gold.order_item / gold.order_payment, so totals reconcile exactly.
#}

with orders as (

    select * from {{ ref('stg_olist__orders') }}

),

-- per-order customer record: the person + the delivery address of THIS order
order_customers as (

    select customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
    from {{ ref('stg_olist__customers') }}

),

delivery_geo as (

    select * from {{ ref('int_geolocation__zip_prefix') }}

),

-- single definition of the analysis period lives in the calendar (vars) - reuse it, don't repeat it
calendar as (

    select calendar_date, is_in_analysis_period from {{ ref('calendar') }}

),

-- 1) items rolled up to one row per order
item_summary as (

    select
        order_id,
        count(*)                        as item_count,          -- units (no quantity column in source)
        count(distinct product_id)      as product_count,
        count(distinct seller_id)       as seller_count,
        sum(item_price_brl)             as items_value_brl,
        sum(freight_price_brl)          as freight_value_brl,
        sum(item_price_eur)             as items_value_eur,
        sum(freight_price_eur)          as freight_value_eur,
        sum(item_price_czk)             as items_value_czk,
        sum(freight_price_czk)          as freight_value_czk,
        max(distance_km)                as max_seller_distance_km  -- farthest seller of the order
    from {{ ref('order_item') }}
    group by order_id

),

-- 2) payments rolled up to one row per order
payment_summary as (

    select
        order_id,
        count(*)                                        as payment_count,
        sum(payment_amount_brl)                         as payment_total_brl,
        sum(payment_amount_eur)                         as payment_total_eur,
        sum(payment_amount_czk)                         as payment_total_czk,
        max_by(payment_method, payment_amount_brl)      as main_payment_method,   -- method of the largest payment
        max(installment_count)                          as max_installment_count
    from {{ ref('order_payment') }}
    group by order_id

),

-- 3) reviews rolled up to one row per order (some orders have several reviews)
review_summary as (

    select
        order_id,
        count(*)                                        as review_count,
        round(avg(review_score), 2)                     as avg_review_score,
        bool_or(review_sentiment = 'negative')          as has_negative_review    -- at least one 1-2 star review
    from {{ ref('order_review') }}
    group by order_id

)

select
    -- keys
    o.order_id,
    c.customer_unique_id                                as customer_id,         -- person -> gold.customer
    o.customer_id                                       as source_customer_id,  -- source per-order key (traceability only)
    cast(o.ordered_at as date)                          as order_date,          -- -> gold.calendar

    -- delivery address of this order (the source has no registered customer address)
    c.customer_zip_code_prefix                          as delivery_zip_code_prefix,
    initcap(c.customer_city)                            as delivery_city,
    c.customer_state                                    as delivery_state,
    g.latitude                                          as delivery_latitude,
    g.longitude                                         as delivery_longitude,

    -- status and timestamps
    o.order_status,
    o.ordered_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_date,

    -- durations (order-level metrics: average them HERE, never from order_item)
    timestampdiff(HOUR, o.ordered_at, o.approved_at)                              as approval_hours,
    round(timestampdiff(HOUR, o.ordered_at, o.delivered_at) / 24.0, 1)            as delivery_days,
    datediff(cast(o.delivered_at as date), o.estimated_delivery_date)             as delay_days,  -- > 0 late, <= 0 on time

    -- flags for marts (marts filter with WHERE, no CASE)
    o.order_status = 'delivered' and o.delivered_at is not null                   as is_delivered,
    o.order_status = 'canceled'                                                   as is_canceled,
    cast(o.delivered_at as date) > o.estimated_delivery_date                      as is_late,     -- NULL if not delivered ("unknown", not "on time")
    {{ is_revenue_eligible('o.order_status') }}                                   as is_revenue_eligible,
    cal.is_in_analysis_period,

    -- item summary (0 instead of NULL: canceled/unavailable orders can have no items)
    coalesce(i.item_count, 0)                           as item_count,
    coalesce(i.product_count, 0)                        as product_count,
    coalesce(i.seller_count, 0)                         as seller_count,
    coalesce(i.items_value_brl, 0)                      as items_value_brl,
    coalesce(i.freight_value_brl, 0)                    as freight_value_brl,
    coalesce(i.items_value_brl, 0) + coalesce(i.freight_value_brl, 0) as order_value_brl,
    coalesce(i.items_value_eur, 0) + coalesce(i.freight_value_eur, 0) as order_value_eur,
    coalesce(i.items_value_czk, 0) + coalesce(i.freight_value_czk, 0) as order_value_czk,
    i.max_seller_distance_km,

    -- payment summary
    coalesce(p.payment_count, 0)                        as payment_count,
    coalesce(p.payment_total_brl, 0)                    as payment_total_brl,
    coalesce(p.payment_total_eur, 0)                    as payment_total_eur,
    coalesce(p.payment_total_czk, 0)                    as payment_total_czk,
    p.main_payment_method,
    p.max_installment_count,
    -- reconciliation: paid vs ordered (instalment interest and vouchers cause small differences)
    coalesce(p.payment_total_brl, 0)
        - (coalesce(i.items_value_brl, 0) + coalesce(i.freight_value_brl, 0)) as payment_difference_brl,

    -- review summary (has_negative_review is false also without reviews -> check review_count)
    coalesce(r.review_count, 0)                         as review_count,
    r.avg_review_score,
    coalesce(r.has_negative_review, false)              as has_negative_review

from orders o
left join order_customers c  on c.customer_id     = o.customer_id      -- left: never silently drop an order; not_null test flags a missing customer
left join delivery_geo g     on g.zip_code_prefix = c.customer_zip_code_prefix
left join calendar cal       on cal.calendar_date = cast(o.ordered_at as date)
-- each summary is already ONE row per order -> these joins cannot multiply rows
left join item_summary i     on i.order_id = o.order_id
left join payment_summary p  on p.order_id = o.order_id
left join review_summary r   on r.order_id = o.order_id