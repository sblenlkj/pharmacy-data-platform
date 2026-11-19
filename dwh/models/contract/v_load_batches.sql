-- v_load_batches: журнал порций загрузчика (ADR-011), одна строка на batch_id.
select
    batch_id,
    offsets_json,
    status
from {{ source('ctl', 'load_batches') }}
