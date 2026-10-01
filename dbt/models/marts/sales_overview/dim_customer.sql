{#
    Customer dimension for the Sales Overview report (only the columns the report uses).
    Location = last delivery location; sales by state use delivery_state on the facts.
#}

select
    customer_id,
    last_delivery_city,
    last_delivery_state,
    first_order_at,
    last_order_at,
    order_count,
    is_repeat_customer
from {{ ref('customer') }}