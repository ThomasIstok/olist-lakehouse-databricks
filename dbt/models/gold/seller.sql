{#
    Seller dimension. Grain: one row per seller.
    Sellers are independent shops selling through the Olist marketplace (not employees).
    Unlike customers, the source sellers table IS master data: one row per seller with the
    seller s own location -> the address belongs to the seller here.
#}

with sellers as (

    select * from {{ ref('stg_olist__sellers') }}

),

geo as (

    select * from {{ ref('int_geolocation__zip_prefix') }}

)

select
    s.seller_id,
    s.seller_zip_code_prefix            as zip_code_prefix,
    initcap(s.seller_city)              as city,        -- display casing: "sao paulo" -> "Sao Paulo"
    s.seller_state                      as state,
    g.latitude,
    g.longitude
from sellers s
left join geo g on g.zip_code_prefix = s.seller_zip_code_prefix   -- left: a few sellers have no coordinates