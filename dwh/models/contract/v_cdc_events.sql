-- v_cdc_events: дедуплицированный журнал CDC (доставка at-least-once:
-- row_number по (topic, partition_id, kafka_offset) — в stg_cdc_events).
select
    source_system,
    source_table,
    event_key,
    op,
    topic,
    partition_id,
    kafka_offset,
    ingested_at
from {{ ref('stg_cdc_events') }}
