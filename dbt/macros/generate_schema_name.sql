{#
    dbt's default behaviour concatenates the target schema with any custom
    schema set in dbt_project.yml (e.g. "news_dwh_news_mart"). This project
    wants the custom schema used exactly as written — it already matches the
    schemas created in sql/02_dwh_ddl.sql (news_raw, news_intermediate,
    news_dwh, news_mart) — so we override the macro.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
