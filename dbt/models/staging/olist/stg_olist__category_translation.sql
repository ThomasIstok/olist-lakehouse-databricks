{#
    Portuguese -> English product category names. Grain: one row per Portuguese category.
    The English names contain typos in the source - fixed here (cleaning belongs to staging),
    so every downstream layer works with correct names.
    The Portuguese name (join key to products) is NOT changed.
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

        -- typo fixes in the source English names
        case trim(product_category_name_english)
            when 'fashio_female_clothing'   then 'fashion_female_clothing'
            when 'costruction_tools_garden' then 'construction_tools_garden'
            when 'costruction_tools_tools'  then 'construction_tools_tools'
            when 'home_confort'             then 'home_comfort'
            when 'arts_and_craftmanship'    then 'arts_and_craftsmanship'
            else trim(product_category_name_english)
        end                                              as product_category_name_en,

        _ingested_at,
        element_at(split(_source_file, '/'), -1)         as _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed