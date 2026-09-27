{#
    Staging model for Olist orders: one row per order.
    Typing, renaming and light cleaning only - no joins, no business logic.
#}

with source as (

    select * from {{ source('olist', 'olist_orders') }}

),

-- Defensive deduplication: bronze is append-only, so a re-delivered or corrected
-- source file would create a second row for the same order. Keep the latest ingested one.
deduplicated as (

    select *
    from source
    qualify row_number() over (partition by order_id order by _ingested_at desc) = 1

),

renamed as (

    select
        -- keys
        order_id,
        customer_id,

        -- attributes (lower/trim guards against inconsistent casing or spaces from the source)
        lower(trim(order_status)) as order_status,

        -- timestamps: full precision kept on purpose (e.g. hour-of-day analysis);
        -- truncating to day/month happens later in marts, never here
        cast(order_purchase_timestamp as timestamp)      as ordered_at,
        cast(order_approved_at as timestamp)             as approved_at,
        cast(order_delivered_carrier_date as timestamp)  as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,

        -- source provides only a date for the estimate (time part is always 00:00:00)
        cast(order_estimated_delivery_date as date)      as estimated_delivery_date,

        -- audit columns: trace every row back to its file and pipeline run
        _ingested_at,
        _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed

