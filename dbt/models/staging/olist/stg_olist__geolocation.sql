{#
    Staging model for Olist geolocation.
    Source has ~1M rows: many exact duplicates and dozens of coordinates per zip prefix,
    plus a few points outside Brazil (source errors).
    Here: remove exact duplicates, type, flag outliers. Aggregation to one row per zip prefix
    happens in intermediate (int_geolocation__zip_prefix) to avoid fan-out in joins.
#}

with source as (

    select * from {{ source('olist', 'olist_geolocation') }}

),

typed as (

    select distinct   -- exact duplicate rows carry no information
        lpad(trim(geolocation_zip_code_prefix), 5, '0')  as zip_code_prefix,
        -- coordinates: double is fine (not money; tiny float error is irrelevant for maps/distances)
        cast(geolocation_lat as double)                  as latitude,
        cast(geolocation_lng as double)                  as longitude,
        lower(trim(geolocation_city))                    as city,
        upper(trim(geolocation_state))                   as state
    from source

)

select
    *,
    -- rough bounding box of Brazil; points outside are source errors and are excluded downstream
    (latitude between -34 and 6 and longitude between -74 and -34) as is_within_brazil
from typed