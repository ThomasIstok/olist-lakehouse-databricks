{#
    Order fact for the Sales Overview report. Grain: one row per order.
    Order counts, average order value, delivery and review metrics come from here -
    never from fct_order_item (an order with 3 units would count 3 times).
    Only SELECT + WHERE on gold.
#}

select
    -- keys -> dimensions
    order_id,
    customer_id,          -- -> dim_customer
    order_date,           -- -> dim_calendar

    -- delivery address of the order
    delivery_state,
    delivery_city,

    -- status and filters
    order_status,
    is_revenue_eligible,
    is_delivered,
    is_canceled,
    is_late,

    -- delivery metrics (average these here)
    approval_hours,
    delivery_days,
    delay_days,

    -- order value
    item_count,
    order_value_brl,
    order_value_eur,
    order_value_czk,

    -- payment summary
    payment_total_brl,
    main_payment_method,
    max_installment_count,

    -- review summary
    review_count,
    avg_review_score,
    has_negative_review

from {{ ref('sales_order') }}
where is_in_analysis_period