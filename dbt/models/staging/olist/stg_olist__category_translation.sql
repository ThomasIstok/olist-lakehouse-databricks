{#
    Portuguese -> English product category names. Grain: one row per Portuguese category.
#}

with source as (

    select * from {{ source('olist', 'olist_category_translation') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (partition by product_category_name order by _ingested_at desc) = 1

),

renamed as (

    select
        trim(product_category_name)                      as product_category_name,
        trim(product_category_name_english)              as product_category_name_en,

        _ingested_at,
        element_at(split(_source_file, '/'), -1)         as _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed