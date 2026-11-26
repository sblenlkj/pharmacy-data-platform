{#
 Разбор CDC-событий в staging.

 Контракт payload (задаёт ingest-сервис): колонка
 raw.cdc_events.payload хранит JSON-объект envelope Debezium после снятия
 обёртки {schema, payload}: {"before": {...}, "after": {...}, "op": "r|c|u|d"}.
 Ключи Kafka (event_key) — PK физической таблицы источника; бизнес-склейка
 идёт по BK из after (dwh_mapping.md §10).

 Представление типов Debezium (time.precision.mode=adaptive,
 decimal.handling.mode=string — подтверждено реальным снапшотом):
 timestamptz → ISO-8601 строка с зоной и микросекундами (ZonedTimestamp);
 date → целое число дней от 1970-01-01 (Date);
 numeric → строка; boolean → JSON-boolean.
#}

{# Поле объекта after как varchar; NULL для отсутствующих/NULL полей. #}
{% macro after(field) -%}
 nullif(json_extract_scalar(json_parse(payload), '$.after.{{ field }}'), '')
{%- endmacro %}

{% macro after_int(field) -%}
 cast({{ after(field) }} as integer)
{%- endmacro %}

{# timestamp источника: Debezium ZonedTimestamp — ISO-8601 с зоной и
 микросекундами ('2025-01-31T00:00:00.123456Z'). Прямой cast ISO-строки
 Trino отвергает (INVALID_CAST_ARGUMENT), а from_iso8601_timestamp режет
 точность до миллисекунд; замена 'T' на пробел даёт cast с точностью 6
 (суффикс Z Trino понимает как UTC). #}
{% macro after_ts(field) -%}
 cast(replace({{ after(field) }}, 'T', ' ') as timestamp(6) with time zone)
{%- endmacro %}

{# date источника: Debezium (adaptive) отдаёт DATE числом дней от epoch;
 ISO-текст оставлен как fallback на случай смены конвертера. #}
{% macro after_date(field) -%}
 case
 when regexp_like({{ after(field) }}, '^\d+$')
 then date_add('day', cast({{ after(field) }} as integer), date '1970-01-01')
 else cast({{ after(field) }} as date)
 end
{%- endmacro %}

{% macro after_bool(field) -%}
 cast({{ after(field) }} as boolean)
{%- endmacro %}

{% macro after_dec(field, precision=18, scale=2) -%}
 cast({{ after(field) }} as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}

{#
 Окно порции загрузчика (ADR-011): var batch_ranges — список диапазонов
 [{"topic", "partition", "start_offset", "end_offset"}] от scripts/load_dds
 (--vars). Отсутствие var = окно не задано (ручной dbt run
 без порции): фильтр вырождается в true.
#}
{% macro batch_filter(alias='') -%}
 {%- set ranges = var('batch_ranges', none) -%}
 {%- if ranges is none -%}
true
 {%- elif not ranges -%}
false
 {%- else -%}
 {%- set by_topic = {} -%}
 {%- for r in ranges -%}
 {%- set _ = by_topic.setdefault(r.topic, []).append(r) -%}
 {%- endfor -%}
(
 {%- for topic, rs in by_topic.items() %}
 ({{ alias }}topic = '{{ topic }}' and (
 {%- for r in rs %}
 ({{ alias }}partition_id = {{ r.partition }}
 and {{ alias }}kafka_offset >= {{ r.start_offset }}
 and {{ alias }}kafka_offset < {{ r.end_offset }}){{ ' or' if not loop.last }}
 {%- endfor %}
 )){{ ' or' if not loop.last }}
 {%- endfor %}
)
 {%- endif -%}
{%- endmacro %}

{#
 Текущее обработанное состояние, а не окно raw. История версионируемых
 таблиц сохраняется отдельными PK. Фильтр порции применяется в DDS.
 source_pk/filtered оставлены для совместимости с typed-wrapper вызовами.
#}
{% macro cdc_latest(source_system, source_table, source_pk, body, filtered=true) -%}
select
 {{ body | join(',\n') }}
from {{ ref('stg_source_state') }}
where topic = '{{ source_system }}.{{ source_table }}'
  and op in ('r', 'c', 'u')
{%- endmacro %}

{# Поддерживает NULL от LEFT JOIN; пустое заданное окно = ни одной строки. #}
{% macro changed(alias) -%}
coalesce({{ batch_filter(alias ~ '.') }}, false)
{%- endmacro %}

{# Аудит-колонки lineage для dim/fact (dwh_mapping.md §10):
 основной источник — событие строки (e — алиас обёртки строки факта). #}
{% macro lineage_columns(e, indent=' ') -%}
{{ indent }}'{{ var("load_batch_id") | replace("'", "''") }}' as _load_batch_id,
{{ indent }}{{ e }}.topic as _src_topic,
{{ indent }}{{ e }}.partition_id as _src_partition,
{{ indent }}{{ e }}.kafka_offset as _src_offset
{%- endmacro %}
