{#
    Order item fact. Grain: one row per UNIT sold (Olist has no quantity column).
    Main sales fact: revenue by category, seller, region and time comes from here.
    - keys to every dimension: product, seller, customer (person), calendar
    - delivery state/city of THIS order (region analysis; the address belongs to the order)
    - prices in BRL / EUR / CZK (FX rule: rate of the order date, same as other facts)
    - distance seller -> delivery location of this order
    - flags for marts (marts only filter with WHERE, no CASE)
#}

with items as (

    select * from {{ ref('stg_olist__order_items') }}

),

orders as (

    select order_id, customer_id, order_status, ordered_at from {{ ref('stg_olist__orders') }}

),

-- per-order customer record: the person AND the delivery address of this order
order_customers as (

    select customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
    from {{ ref('stg_olist__customers') }}

),

delivery_geo as (

    select * from {{ ref('int_geolocation__zip_prefix') }}

),

sellers as (

    select seller_id, latitude, longitude from {{ ref('seller') }}

),

fx as (

    select * from {{ ref('fx_rate_daily') }}

),

-- analysis period is defined once in the calendar (vars) - reuse it, don't repeat it
calendar as (

    select calendar_date, is_in_analysis_period from {{ ref('calendar') }}

)

select
    -- keys
    {{ dbt_utils.generate_surrogate_key(['i.order_id', 'i.order_item_number']) }} as order_item_key,  -- single-column key for BI
    i.order_id,
    i.order_item_number,
    i.product_id,                                                   -- -> gold.product
    i.seller_id,                                                    -- -> gold.seller
    c.customer_unique_id                                            as customer_id,  -- person -> gold.customer
    cast(o.ordered_at as date)                                      as order_date,   -- -> gold.calendar

    -- delivery address of this order (for revenue by state / city)
    initcap(c.customer_city)                                        as delivery_city,
    c.customer_state                                                as delivery_state,

    -- order context as FILTER attributes (do not average order metrics here - use gold.sales_order)
    o.order_status,
    {{ is_revenue_eligible('o.order_status') }}                     as is_revenue_eligible,
    cal.is_in_analysis_period,

    i.shipping_limit_at,

    -- money in BRL as sold, converted with the order-date rate; decimal keeps exact cents
    i.item_price_brl,
    i.freight_price_brl,
    i.item_price_brl + i.freight_price_brl                          as item_total_brl,
    cast(i.item_price_brl    * fx.brl_to_eur as decimal(12, 2))     as item_price_eur,
    cast(i.freight_price_brl * fx.brl_to_eur as decimal(12, 2))     as freight_price_eur,
    cast(i.item_price_brl    * fx.brl_to_czk as decimal(12, 2))     as item_price_czk,
    cast(i.freight_price_brl * fx.brl_to_czk as decimal(12, 2))     as freight_price_czk,

    -- distance seller -> delivery location of this order (NULL if a zip has no coordinates)
    round({{ haversine_km('s.latitude', 's.longitude', 'g.latitude', 'g.longitude') }}, 1) as distance_km

from items i
left join orders o            on o.order_id        = i.order_id      -- left: never silently drop an item; tests flag missing links
left join order_customers c   on c.customer_id     = o.customer_id
left join delivery_geo g      on g.zip_code_prefix = c.customer_zip_code_prefix
left join sellers s           on s.seller_id       = i.seller_id
left join fx                  on fx.calendar_date  = cast(o.ordered_at as date)   -- left: never drop revenue for a missing rate
left join calendar cal        on cal.calendar_date = cast(o.ordered_at as date)