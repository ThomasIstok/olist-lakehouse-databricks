{#
    Staging model for Olist customers.
    Grain: one row per customer_id. NOTE the confusing source naming:
      - customer_id        = key created PER ORDER (1:1 with orders), not a person
      - customer_unique_id = the real person; use it for repeat-customer analysis
#}

with source as (

    select * from {{ source('olist', 'olist_customers') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (partition by customer_id order by _ingested_at desc) = 1

),

renamed as (

    select
        -- keys
        customer_id,                                                   -- per-order key, joins to orders
        customer_unique_id,                                            -- real person

        -- location: zip prefix is text; source lost leading zeros (1310 -> 01310), restore them
        lpad(trim(customer_zip_code_prefix), 5, '0')  as customer_zip_code_prefix,
        lower(trim(customer_city))                    as customer_city,  -- display casing done in marts
        upper(trim(customer_state))                   as customer_state, -- 2-letter state code, e.g. SP

        -- audit (file name derived: reference tables were loaded before _source_file_name existed)
        _ingested_at,
        element_at(split(_source_file, '/'), -1)      as _source_file_name,
        _batch_id

    from deduplicated

)

select * from renamed