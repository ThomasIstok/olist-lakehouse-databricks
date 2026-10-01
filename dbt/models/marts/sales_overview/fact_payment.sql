{#
    Payment fact for the Sales Overview report. Grain: one row per payment.
    Payment method split (credit card / boleto bank slip / voucher / debit card)
    and instalments come from here. Only SELECT + WHERE on gold.
#}

select
    -- keys -> dimensions
    order_id,
    payment_sequence,
    customer_id,          -- -> dim_customer
    order_date,           -- -> dim_calendar

    -- payment attributes
    payment_method,
    installment_count,
    is_installment,

    -- measures
    payment_amount_brl,
    payment_amount_eur,
    payment_amount_czk

from {{ ref('order_payment') }}
where is_in_analysis_period