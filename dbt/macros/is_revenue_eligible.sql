{#
    Single definition of "counts as revenue", reused by every fact (order_item, order).
    Business rule: everything except canceled and unavailable orders
    (shipped/invoiced orders in progress are revenue too).
    Change it here -> all facts and reports follow.
#}
{% macro is_revenue_eligible(status_column) %}
    ({{ status_column }} not in ('canceled', 'unavailable'))
{% endmacro %}