{# Схема без суффикса target.schema: contract_dwh остаётся contract_dwh
   (имя схемы контракта фиксировано чекером), а не dwh_detailed_contract_dwh. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
