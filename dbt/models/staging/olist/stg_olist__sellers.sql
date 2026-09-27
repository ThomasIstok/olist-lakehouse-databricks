{#
    Staging model for Olist sellers. Grain: one row per seller.
#}

with source as (

    select * from {{ source('olist', 'olist_sellers') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (partition by seller_id order by _ingested_at desc) = 1

),

renamed as (

    select
        seller_id,

        -- same location cleanup as customers, so zip prefixes join consistently to geolocation
        lpad(trim(seller_zip_code_prefix), 5, '0')    as seller_zip_code_prefix,
        lower(trim(seller_city))                      as seller_city,
        upper(trim(seller_state))                     as seller_state,

        _ingested_at,
        element_at(split(_source_file, '/'), -1)      as _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed