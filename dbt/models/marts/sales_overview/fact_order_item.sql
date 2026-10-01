{#
    Sales fact for the Sales Overview report. Grain: one row per unit sold.
    Revenue by category / seller / state / day comes from here.
    Only SELECT + WHERE on gold (no new logic). Canceled orders are KEPT with
    is_revenue_eligible = false, so the report can also show cancellations;
    revenue measures filter on that flag.
#}

select
    -- keys -> dimensions
    order_item_key,
    order_id,
    order_item_number,
    product_id,           -- -> dim_product
    seller_id,            -- -> dim_seller
    customer_id,          -- -> dim_customer
    order_date,           -- -> dim_calendar

    -- delivery address of the order (sales by state / city)
    delivery_state,
    delivery_city,

    -- filters
    order_status,
    is_revenue_eligible,

    -- measures (BRL as sold, EUR / CZK at the order-date rate)
    item_price_brl,
    freight_price_brl,
    item_total_brl,
    item_price_eur,
    freight_price_eur,
    item_price_czk,
    freight_price_czk,
    distance_km

from {{ ref('order_item') }}
where is_in_analysis_period   -- reliable data only (edge months are incomplete exports)