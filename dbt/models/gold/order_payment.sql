{#
    Payment fact. Grain: one row per payment of an order (an order can be split,
    e.g. voucher + credit card).
    Adds the keys needed to join dimensions in BI (customer = person, order date -> calendar)
    and converts amounts to EUR / CZK.
    FX rule (same in every fact): rate of the ORDER DATE, so amounts across facts reconcile.
#}

with payments as (

    select * from {{ ref('stg_olist__order_payments') }}

),

orders as (

    select order_id, customer_id, ordered_at from {{ ref('stg_olist__orders') }}

),

-- source customer_id is per order -> map it to the real person (gold customer_id)
customers as (

    select customer_id, customer_unique_id from {{ ref('stg_olist__customers') }}

),

fx as (

    select * from {{ ref('fx_rate_daily') }}

)

select
    -- keys
    p.order_id,
    p.payment_sequence,
    c.customer_unique_id                                         as customer_id,   -- person -> gold.customer
    cast(o.ordered_at as date)                                   as order_date,    -- -> gold.calendar

    -- attributes
    p.payment_method,
    p.installment_count,
    p.installment_count > 1                                      as is_installment, -- paid in instalments

    -- amounts: BRL as paid, converted with the order-date rate; decimal keeps exact cents
    p.payment_amount_brl,
    cast(p.payment_amount_brl * fx.brl_to_eur as decimal(12, 2)) as payment_amount_eur,
    cast(p.payment_amount_brl * fx.brl_to_czk as decimal(12, 2)) as payment_amount_czk

from payments p
left join orders o    on o.order_id      = p.order_id         -- left: never silently drop a payment; tests flag missing links
left join customers c on c.customer_id   = o.customer_id
left join fx          on fx.calendar_date = cast(o.ordered_at as date)   -- left: never drop a payment for a missing rate