{# Партиционирование — свойством WITH таблицы Iceberg (dbt-trino 1.12
    не знает partition_by). Спец по ADR-010: month(moved_at). #}
{{ config(
    unique_key='movement_bk',
    properties={
    'partitioning': "ARRAY['month(moved_at)']",
    }
) }}

-- fact_movement: движение партии; sku — из справочника партий (batch),
-- в событии движения его нет (§9). Endpoints полиморфные — denormalized
-- атрибуты факта; ссылочная целостность по типу проверяют
-- представления/чекер. Партиция month(moved_at).
--
-- Инкремент: merge по movement_bk; строки — stg_wms_movement
-- в окне порции. Партия часто старше окна: событие batch в окне имеет
-- приоритет, fallback — уже загруженный dim_batch (DAG гарантирует, что
-- dim_batch пересобран до факта).
select
    m.movement_bk,
    m.batch_bk,
    coalesce(b.sku, db.sku) as sku,
    m.movement_type,
    m.src_type,
    m.src_bk,
    m.dst_type,
    m.dst_bk,
    m.qty,
    m.dispatched_at,
    m.moved_at,
    {{ lineage_columns('m') }}
from {{ ref('stg_wms_movement') }} m
left join {{ ref('stg_wms_batch') }} b on b.batch_bk = m.batch_bk
left join {{ ref('dim_batch') }} db on db.batch_bk = m.batch_bk
