{#
    Drop all schemas created for one pull request (ci_pr<number>_silver, _gold, ...).
    Called by CI at the end of every PR build:
        dbt run-operation drop_pr_schemas --target pr

    Safety: refuses to run outside target "pr" and only drops schemas whose name
    starts with "ci_pr", so it can never touch silver, gold or the marts.
#}
{% macro drop_pr_schemas() %}
    {% if target.name != 'pr' or not target.schema.startswith('ci_pr') %}
        {{ exceptions.raise_compiler_error(
            "drop_pr_schemas runs only with target 'pr' and a schema starting with 'ci_pr', got "
            ~ target.name ~ " / " ~ target.schema
        ) }}
    {% endif %}

    {# Find all schemas of this PR, e.g. ci_pr27_silver, ci_pr27_gold, ... #}
    {% set schemas = run_query(
        "SHOW SCHEMAS IN " ~ target.database ~ " LIKE '" ~ target.schema ~ "_*'"
    ) %}

    {# Drop each one including everything inside it (CASCADE) #}
    {% for row in schemas.rows %}
        {% set drop_sql = "DROP SCHEMA IF EXISTS " ~ target.database ~ "." ~ row[0] ~ " CASCADE" %}
        {% do log(drop_sql, info=True) %}
        {% do run_query(drop_sql) %}
    {% endfor %}

    {% do log("Dropped " ~ (schemas.rows | length) ~ " schema(s) for " ~ target.schema, info=True) %}
{% endmacro %}