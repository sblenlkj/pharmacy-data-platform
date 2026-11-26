{{ config(
    materialized='incremental',
    schema='dwh_staging',
    incremental_strategy='merge',
    unique_key=['topic', 'partition_id', 'event_key'],
    properties={'partitioning': "ARRAY['topic']"}
) }}

-- One processed latest event per physical PK. Input is ONLY the saved raw
-- window. A late replay cannot replace a state row with a lower offset.
-- The stack keeps one partition per source topic; load_dds validates this.
with ranked as (
    select source_system, source_table, event_key, op, topic,
           partition_id, kafka_offset, payload, ingested_at,
           row_number() over (
               partition by topic, partition_id, event_key
               order by kafka_offset desc, ingested_at asc
           ) as rn
    from {{ source('raw', 'cdc_events') }}
    where {{ batch_filter() }}
),
candidates as (
    select source_system, source_table, event_key, op, topic,
           partition_id, kafka_offset, payload, ingested_at
    from ranked where rn = 1
)
select c.*
from candidates c
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.topic = c.topic and t.partition_id = c.partition_id
      and t.event_key = c.event_key and t.kafka_offset >= c.kafka_offset
)
{% endif %}
