{#
    Staging model for Olist products. Grain: one row per product.
    Product names are anonymised in the source - only name/description LENGTH is available.
    Source column names contain a typo ("lenght") - fixed here.
    English category name is joined later in gold (staging = one source table, no joins).
#}

with source as (

    select * from {{ source('olist', 'olist_products') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (partition by product_id order by _ingested_at desc) = 1

),

renamed as (

    select
        product_id,

        -- Portuguese category name; natural key to the category translation table
        nullif(trim(product_category_name), '')          as product_category_name,

        -- text metadata (typo in source: "lenght")
        cast(product_name_lenght as int)                 as product_name_length,
        cast(product_description_lenght as int)          as product_description_length,
        cast(product_photos_qty as int)                  as product_photo_count,

        -- physical attributes: unit kept in the name
        cast(product_weight_g as int)                    as product_weight_g,
        cast(product_length_cm as int)                   as product_length_cm,
        cast(product_height_cm as int)                   as product_height_cm,
        cast(product_width_cm as int)                    as product_width_cm,

        _ingested_at,
        element_at(split(_source_file, '/'), -1)         as _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed