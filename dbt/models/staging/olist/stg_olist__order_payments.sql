{#
    Staging model for Olist order payments.
    Grain: one row per payment of an order. An order can be paid in several parts
    (e.g. voucher + credit card) -> payment_sequential 1, 2, 3 ...
    Payments relate to ORDERS, not to items: never join items and payments directly
    (fan-out would multiply rows and inflate revenue).
#}

with source as (

    select * from {{ source('olist', 'olist_order_payments') }}

),

-- Defensive deduplication on the composite key (order + payment sequence)
deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by order_id, payment_sequential
        order by _ingested_at desc
    ) = 1

),

renamed as (

    select
        -- keys
        order_id,
        cast(payment_sequential as int)         as payment_sequence,   -- 1st, 2nd ... payment of the order

        -- attributes
        lower(trim(payment_type))               as payment_method,     -- credit_card, boleto, voucher ...
        cast(payment_installments as int)       as installment_count,  -- number of card instalments

        -- money: decimal for exact cents, currency in the name
        cast(payment_value as decimal(10, 2))   as payment_amount_brl,

        -- audit columns
        _ingested_at,
        _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed