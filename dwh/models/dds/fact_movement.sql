{{ config(
    unique_key='movement_bk',
    properties={'partitioning': "ARRAY['month(moved_at)']"}
) }}

select
    m.movement_bk, m.batch_bk, b.sku, m.movement_type,
    m.src_type, m.src_bk, m.dst_type, m.dst_bk,
    m.qty, m.dispatched_at, m.moved_at,
    {{ lineage_columns('m') }}
from {{ ref('stg_wms_movement') }} m
left join {{ ref('stg_wms_batch') }} b on b.batch_bk = m.batch_bk
where {{ changed('m') }} or {{ changed('b') }}
