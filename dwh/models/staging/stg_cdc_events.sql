-- Дедупликация доставки at-least-once: одна строка на координату
-- (topic, partition_id, kafka_offset), первый доставленный экземпляр
-- (минимальный ingested_at). Единая точка дедупа для всех staging-обёрток
-- и представления контракта v_cdc_events.
select
    source_system,
    source_table,
    event_key,
    op,
    topic,
    partition_id,
    kafka_offset,
    payload,
    ingested_at
from (
    select
        *,
        row_number() over (
            partition by topic, partition_id, kafka_offset
            order by ingested_at
        ) as _dup_rank
    from {{ source('raw', 'cdc_events') }}
)
where _dup_rank = 1
