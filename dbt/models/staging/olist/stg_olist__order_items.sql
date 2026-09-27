{#
    Staging model for Olist order items.
    Grain: one row per UNIT sold. Olist has no quantity column - buying 3 pieces
    of a product creates 3 rows (order_item_number 1, 2, 3). Quantity = COUNT(*).
#}

with source as (

    select * from {{ source('olist', 'olist_order_items') }}

),

-- Defensive deduplication on the composite key (order + item position)
deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by order_id, order_item_id
        order by _ingested_at desc
    ) = 1

),

renamed as (

    select
        -- keys
        order_id,
        cast(order_item_id as int)             as order_item_number,  -- position within the order, not an ID
        product_id,
        seller_id,

        -- deadline for the seller to hand the item over to the carrier
        cast(shipping_limit_date as timestamp) as shipping_limit_at,

        -- money: decimal, never int (loses cents) or double (rounding errors);
        -- currency suffix because EUR/CZK conversions will be added later
        cast(price as decimal(10, 2))          as item_price_brl,
        cast(freight_value as decimal(10, 2))  as freight_price_brl,

        -- audit columns
        _ingested_at,
        _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed

