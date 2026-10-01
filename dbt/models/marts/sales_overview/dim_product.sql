{#
    Product dimension for the Sales Overview report: readable names and category levels only
    (physical attributes are not needed for sales reporting).
#}

select
    product_id,
    product_display_name,
    category_label,
    category_group,
    category_name
from {{ ref('product') }}