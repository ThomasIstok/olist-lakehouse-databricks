{#
    Staging model for Olist order reviews.
    Grain: one row per review AND order. The same review_id can cover several orders
    (a customer reviewing multiple orders at once) - so review_id alone is NOT unique.
    Sentiment / CX scoring is business logic and belongs to gold, not here.
#}

with source as (

    select * from {{ source('olist', 'olist_order_reviews') }}

),

-- Deduplicate on the real key (review + order); deduplicating on review_id alone would drop real data
deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by review_id, order_id
        order by _ingested_at desc
    ) = 1

),

renamed as (

    select
        -- keys
        review_id,
        order_id,

        -- score 1-5 given by the customer
        cast(review_score as int)                        as review_score,

        -- free text in Portuguese; empty strings -> NULL so "has comment" checks are reliable
        nullif(trim(review_comment_title), '')           as review_title,
        nullif(trim(review_comment_message), '')         as review_message,

        -- survey sent to the customer (date only in source) and when the customer answered
        cast(review_creation_date as date)               as review_created_date,
        cast(review_answer_timestamp as timestamp)       as review_answered_at,

        -- audit columns
        _ingested_at,
        _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed