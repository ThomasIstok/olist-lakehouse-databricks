{#
    One row per zip code prefix with a representative coordinate.
    Joining customers/sellers directly to stg_olist__geolocation would multiply rows
    (dozens of coordinates per prefix) - this model makes the join 1:1.
#}

select
    zip_code_prefix,
    avg(latitude)            as latitude,
    avg(longitude)           as longitude,
    count(*)                 as source_point_count   -- how many distinct points were averaged
from {{ ref('stg_olist__geolocation') }}
where is_within_brazil       -- ignore outliers so they don't distort the average
group by zip_code_prefix