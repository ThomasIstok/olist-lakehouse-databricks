{#
    Review fact. Grain: one row per review AND order (same review can cover several orders).
    Business logic for review analysis lives here:
      - sentiment from the 1-5 score (marts only filter on it, no CASE in marts)
      - has_comment flag for text analysis of the worst reviews
      - response time of the customer to the satisfaction survey
#}

with order_reviews as (

    select * from {{ ref('stg_olist__order_reviews') }}

),

review_enriched as (

    select
        review_id,
        order_id,
        review_score,

        -- score is an integer 1-5 -> exact boundaries (a range like < 2.5 would never yield neutral)
        case
            when review_score <= 2 then 'negative'
            when review_score = 3  then 'neutral'
            else 'positive'
        end                                                       as review_sentiment,

        -- boolean like all our flags; a comment can be only a title or only a message
        (review_title is not null or review_message is not null)  as has_comment,
        review_title,
        review_message,

        review_created_date,                                      -- survey sent to the customer (date only)
        review_answered_at,                                       -- customer answered (timestamp)

        -- hours between survey sent (midnight of the creation date) and the answer
        timestampdiff(
            HOUR,
            cast(review_created_date as timestamp),
            review_answered_at
        )                                                         as response_hours

    from order_reviews

)

select * from review_enriched