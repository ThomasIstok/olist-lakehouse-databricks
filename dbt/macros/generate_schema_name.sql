{#
    How dbt names schemas.

    Normal runs (local dev, prod Job): use the custom schema from dbt_project.yml as-is
    ("silver", "gold", ...). dbt default would prefix it with the target schema
    ("silver_gold"), which we don't want.

    Pull request runs in CI (target "pr"): prefix it with the target schema,
    which CI sets to "ci_pr<number>". Every PR gets its own schemas
    (ci_pr27_silver, ci_pr27_gold, ...), so a CI build never overwrites
    tables someone works with, and two PRs never collide.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'pr' -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}