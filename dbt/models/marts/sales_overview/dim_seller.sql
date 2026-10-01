{#
    Seller dimension for the Sales Overview report. Coordinates kept for a seller map.
#}

select
    seller_id,
    city,
    state,
    latitude,
    longitude
from {{ ref('seller') }}