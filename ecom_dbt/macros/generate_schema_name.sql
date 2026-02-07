{# https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-schema-customization #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {# If you defined a schema in your config, use it exactly #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}

    {# Otherwise, fall back to the default schema from your profiles.yml #}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}