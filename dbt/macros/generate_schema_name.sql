{#
    Use the custom schema from dbt_project.yml as-is (e.g. "silver", "gold").
    dbt default would prefix it with the target schema -> "silver_gold", which we don't want.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
